//! # at-cmux
//!
//! Implementation of 3GPP TS 27.010 CMUX (GSM multiplexer protocol) for embedded systems.
//!
//! Based on https://www.3gpp.org/ftp/tsg_t/tsg_t/tsgt_04/docs/pdfs/TP-99119.pdf
//!
//! ## Features
//!
//! - `defmt` - Enable defmt logging
//! - `log` - Enable log crate logging
//!
//! ## Interoperability
//!
//! This crate can run on any async executor.
//!
//! It supports any serial port implementing [`embedded-io-async`](https://crates.io/crates/embedded-io-async).

#![cfg_attr(not(test), no_std)]

mod fmt;
mod frame;

use core::cell::Cell;
use core::future::{poll_fn, Future};
use core::mem::MaybeUninit;
use core::pin::pin;
use core::task::Poll;

use embassy_futures::select::{select, select_slice, Either};
use embassy_sync::blocking_mutex::raw::NoopRawMutex;
use embassy_sync::mutex::Mutex;
use embassy_sync::pipe::{Pipe, Reader, Writer};
use embassy_sync::signal::Signal;
use embassy_sync::waitqueue::AtomicWaker;
pub use frame::{Break, Control};
use frame::{Frame, Information, MultiplexerCloseDown};
use heapless::Vec;

use bbqueue::prod_cons::framed::{FramedConsumer, FramedGrantR, FramedProducer};
use bbqueue::traits::coordination::cs::CsCoord;
use bbqueue::traits::notifier::maitake::MaiNotSpsc;
use bbqueue::traits::storage::Inline;
use bbqueue::BBQueue;

use crate::frame::{Error, FrameType, NonSupportedCommandResponse};

/// RX queue type: inline storage, critical-section coordination, maitake async notification.
type RxQueue<const BUF: usize> = BBQueue<Inline<BUF>, CsCoord, MaiNotSpsc>;

struct Lines {
    rx: Cell<(Control, Option<Break>)>,
    tx: Cell<(Control, Option<Break>)>,
    opened: Cell<bool>,
    pending_command: Cell<Option<FrameType>>,
    hangup: Cell<bool>,
    hangup_mask: Cell<Option<(u16, u16)>>,
    hangup_waker: AtomicWaker,
}

impl Lines {
    const fn new() -> Self {
        Self {
            rx: Cell::new((Control::new(), None)),
            tx: Cell::new((Control::new(), None)),
            opened: Cell::new(false),
            pending_command: Cell::new(None),
            hangup: Cell::new(false),
            hangup_mask: Cell::new(None),
            hangup_waker: AtomicWaker::new(),
        }
    }

    fn check_hangup(&self) {
        if !self.rx.get().0.dv() && !self.hangup.get() {
            let (control, _brk) = self.rx.get();
            warn!(
                "HANGUP detected! Control: rtc={} rtr={} ic={} dv={}, mask={:?}",
                control.rtc(),
                control.rtr(),
                control.ic(),
                control.dv(),
                self.hangup_mask.get()
            );
        }
    }
}

/// Main CMUX multiplexer.
///
/// Generic parameters:
/// - `N`: Number of logical channels
/// - `BUF`: Buffer size for each channel's TX/RX pipes
pub struct Mux<const N: usize, const BUF: usize> {
    tx: [Pipe<NoopRawMutex, BUF>; N],
    rx: [RxQueue<BUF>; N],
    lines: [Lines; N],
    line_status_updated: Signal<NoopRawMutex, ()>,
}

/// A logical CMUX channel for reading and writing data.
pub struct Channel<'a, const BUF: usize> {
    rx: FramedBufReader<'a, BUF>,
    tx: Writer<'a, NoopRawMutex, BUF>,
    lines: &'a Lines,
    line_status_updated: &'a Signal<NoopRawMutex, ()>,
}

/// Receive half of a CMUX channel.
pub struct ChannelRx<'a, const BUF: usize> {
    rx: FramedBufReader<'a, BUF>,
    lines: &'a Lines,
    line_status_updated: &'a Signal<NoopRawMutex, ()>,
}

/// BufRead adapter over a framed bbqueue consumer.
///
/// Each frame in the queue is one FCS-validated CMUX data payload.
/// `fill_buf` returns bytes from the current frame; `consume` advances
/// within it. When a frame is fully consumed, it is released and the
/// next `fill_buf` waits for the next frame.
struct FramedBufReader<'a, const BUF: usize> {
    consumer: FramedConsumer<&'a RxQueue<BUF>, u16>,
    current_grant: Option<FramedGrantR<&'a RxQueue<BUF>, u16>>,
    offset: usize,
}

impl<'a, const BUF: usize> FramedBufReader<'a, BUF> {
    fn new(consumer: FramedConsumer<&'a RxQueue<BUF>, u16>) -> Self {
        Self {
            consumer,
            current_grant: None,
            offset: 0,
        }
    }

    async fn fill_buf(&mut self) -> &[u8] {
        if self.current_grant.is_none() {
            self.current_grant = Some(self.consumer.wait_read().await);
            self.offset = 0;
        }
        &self.current_grant.as_ref().unwrap()[self.offset..]
    }

    fn consume(&mut self, amt: usize) {
        self.offset += amt;
        if let Some(ref grant) = self.current_grant {
            if self.offset >= grant.len() {
                self.current_grant.take().unwrap().release();
                self.offset = 0;
            }
        }
    }

    async fn read(&mut self, buf: &mut [u8]) -> usize {
        let data = self.fill_buf().await;
        let n = data.len().min(buf.len());
        buf[..n].copy_from_slice(&data[..n]);
        self.consume(n);
        n
    }
}

/// Transmit half of a CMUX channel.
pub struct ChannelTx<'a, const BUF: usize> {
    tx: Writer<'a, NoopRawMutex, BUF>,
    lines: &'a Lines,
    line_status_updated: &'a Signal<NoopRawMutex, ()>,
}

/// Handle to access modem control lines for a channel.
#[derive(Clone)]
pub struct ChannelLines<'a, const BUF: usize> {
    lines: &'a Lines,
    line_status_updated: &'a Signal<NoopRawMutex, ()>,
}

/// Runner that manages the CMUX protocol.
///
/// Must be run in a background task to handle frame multiplexing.
pub struct Runner<'a, const N: usize, const BUF: usize> {
    tx: [Reader<'a, NoopRawMutex, BUF>; N],
    rx: [FramedProducer<&'a RxQueue<BUF>, u16>; N],
    control_channel_opened: Cell<bool>,
    control_pending_command: Cell<Option<FrameType>>,
    lines: &'a [Lines; N],
    _line_status_updated: &'a Signal<NoopRawMutex, ()>,
}

impl<const N: usize, const BUF: usize> Mux<N, BUF> {
    #[allow(clippy::declare_interior_mutable_const)]
    const ONE_PIPE: Pipe<NoopRawMutex, BUF> = Pipe::new();
    #[allow(clippy::declare_interior_mutable_const)]
    const ONE_RX_QUEUE: RxQueue<BUF> = BBQueue::new();

    /// Create a new CMUX multiplexer.
    #[allow(clippy::borrow_interior_mutable_const)]
    pub const fn new() -> Self {
        #[allow(clippy::declare_interior_mutable_const)]
        const LINE: Lines = Lines::new();

        Self {
            rx: [Self::ONE_RX_QUEUE; N],
            tx: [Self::ONE_PIPE; N],
            lines: [LINE; N],
            line_status_updated: Signal::new(),
        }
    }

    /// Start the multiplexer, returning the runner and channels.
    ///
    /// The runner must be spawned in a background task and given access to
    /// the serial port. The channels can be used to read/write data.
    pub fn start(&mut self) -> (Runner<'_, N, BUF>, [Channel<'_, BUF>; N]) {
        let mut chs = MaybeUninitArray::<_, N>::uninit();
        let mut runner_tx = MaybeUninitArray::<_, N>::uninit();
        let mut runner_rx = MaybeUninitArray::<_, N>::uninit();

        for (i, (tx, rx)) in self.tx.iter_mut().zip(self.rx.iter()).enumerate() {
            let (tx_r, tx_w) = tx.split();
            let ch = Channel {
                rx: FramedBufReader::new(rx.framed_consumer()),
                tx: tx_w,
                lines: &self.lines[i],
                line_status_updated: &self.line_status_updated,
            };
            unsafe {
                chs.set(i, ch);
                runner_rx.set(i, rx.framed_producer());
                runner_tx.set(i, tx_r);
            }
        }
        let runner = Runner {
            rx: unsafe { runner_rx.assume_init() },
            tx: unsafe { runner_tx.assume_init() },
            control_channel_opened: Cell::new(false),
            control_pending_command: Cell::new(None),
            lines: &self.lines,
            _line_status_updated: &self.line_status_updated,
        };
        (runner, unsafe { chs.assume_init() })
    }
}

impl<const N: usize, const BUF: usize> Default for Mux<N, BUF> {
    fn default() -> Self {
        Self::new()
    }
}

impl<'a, const N: usize, const BUF: usize> Runner<'a, N, BUF> {
    /// Run the CMUX protocol handler.
    ///
    /// This method handles all frame multiplexing and should be run in a background task.
    ///
    /// # Arguments
    /// * `port_r` - The serial port reader (must implement `BufRead`)
    /// * `port_w` - The serial port writer (must implement `Write`)
    /// * `max_frame_size` - Maximum size of data frames
    pub async fn run<R: embedded_io_async::BufRead, W: embedded_io_async::Write>(
        &mut self,
        mut port_r: R,
        port_w: W,
        max_frame_size: usize,
    ) -> Result<(), Error> {
        // Capture references to Cell fields for the OnDrop — avoids borrowing self
        let control_opened = &self.control_channel_opened;
        let control_pending = &self.control_pending_command;
        let lines = self.lines;
        let drop_mux = OnDrop::new(|| {
            control_opened.set(false);
            control_pending.set(None);

            for line in lines.iter() {
                line.opened.set(false);
                line.pending_command.set(None);
                line.hangup.set(false);
            }
        });

        let port_w = Mutex::<NoopRawMutex, _>::new(port_w);

        // Send startup SABMs
        if !self.control_channel_opened.get() {
            let mut w = port_w.lock().await;
            if let Err(e) = (frame::Sabm { id: 0 }).write(&mut *w).await {
                error!("Failed to send SABM for control channel: {:?}", e);
                return Err(e);
            }
            self.control_pending_command.set(Some(FrameType::Sabm));
        }

        for channel_id in 0..N {
            if !self.lines[channel_id].opened.get() {
                let mut w = port_w.lock().await;
                if let Err(e) = (frame::Sabm {
                    id: channel_id as u8 + 1,
                })
                .write(&mut *w)
                .await
                {
                    error!(
                        "Failed to send SABM for channel {}: {:?}",
                        channel_id + 1,
                        e
                    );
                    return Err(e);
                }
                self.lines[channel_id]
                    .pending_command
                    .set(Some(FrameType::Sabm));
            }
        }

        // Signal used to wake TX loop when flow control state changes
        let fc_changed: Signal<NoopRawMutex, ()> = Signal::new();

        // Destructure self to allow split borrows: tx_loop needs &mut tx,
        // rx_loop needs &mut rx. Both share lines/control via & references.
        let tx_readers = &mut self.tx;
        let rx_writers = &mut self.rx;
        let lines = self.lines;
        let control_opened = &self.control_channel_opened;
        let control_pending = &self.control_pending_command;

        let tx_fut = tx_loop::<N, BUF, W>(tx_readers, lines, &port_w, &fc_changed, max_frame_size);
        let rx_fut = rx_loop::<N, BUF, R, W>(
            &mut port_r,
            rx_writers,
            lines,
            control_opened,
            control_pending,
            &port_w,
            &fc_changed,
        );

        // Run TX and RX loops concurrently. When either returns, the other is dropped.
        let result = match select(tx_fut, rx_fut).await {
            Either::First(r) => r,
            Either::Second(r) => r,
        };

        // Shutdown: send DISC for open channels, then CLD
        {
            let mut w = port_w.lock().await;
            for id in (0..N).rev() {
                let dlci = id + 1;
                let line = &self.lines[id];
                if line.opened.get() {
                    if let Err(e) = (frame::Disc { id: dlci as u8 }).write(&mut *w).await {
                        error!("Failed to send DISC for channel {}: {:?}", dlci, e);
                    }
                }
            }

            if self.control_channel_opened.get() {
                if let Err(e) = (frame::Uih {
                    id: 0,
                    information: Information::MultiplexerCloseDown(MultiplexerCloseDown {
                        cr: frame::CR::Command,
                    }),
                })
                .write(&mut *w)
                .await
                {
                    error!("Failed to send MultiplexerCloseDown: {:?}", e);
                }
            }
        }

        drop_mux.defuse();
        result
    }
}

async fn tx_loop<const N: usize, const BUF: usize, W: embedded_io_async::Write>(
    tx: &mut [Reader<'_, NoopRawMutex, BUF>; N],
    lines: &[Lines; N],
    port_w: &Mutex<NoopRawMutex, W>,
    fc_changed: &Signal<NoopRawMutex, ()>,
    max_frame_size: usize,
) -> Result<(), Error> {
    loop {
        // Build futures only for channels that are open and not flow-controlled
        let mut futs: Vec<_, N> = Vec::new();
        let mut idx_map: Vec<usize, N> = Vec::new();

        for (i, c) in tx.iter_mut().enumerate() {
            if lines[i].opened.get() && !lines[i].rx.get().0.fc() {
                let _ = futs.push(c.fill_buf());
                let _ = idx_map.push(i);
            }
        }

        if futs.is_empty() {
            // Nothing to send — wait for FC change or yield to let RX loop run
            fc_changed.reset();
            select(fc_changed.wait(), embassy_futures::yield_now()).await;
            continue;
        }

        let (buf, j) = select_slice(pin!(&mut futs)).await;
        let i = idx_map[j];
        let len = buf.len().min(max_frame_size);

        // Lock writer and send UIH frame
        {
            let mut w = port_w.lock().await;
            let frame = frame::Uih {
                id: i as u8 + 1,
                information: Information::Data(&buf[..len]),
            };

            if let Err(e) = frame.write(&mut *w).await {
                error!(
                    "CMUX Runner: CH{} failed to write UIH frame: {:?}",
                    i + 1,
                    e
                );
                return Err(e);
            }
            trace!("CMUX TX: CH{} {} bytes", i + 1, len);
        }

        // Drop futs before consuming (buf borrows from pipe reader)
        drop(futs);
        tx[i].consume(len);
    }
}

/// Send MSC with FC (flow control) bit to pause or resume a specific channel.
async fn send_msc_fc<W: embedded_io_async::Write>(
    port_w: &Mutex<NoopRawMutex, W>,
    channel_id: usize,
    fc_on: bool,
) -> Result<(), Error> {
    let control = frame::Control::new()
        .with_fc(fc_on)
        .with_rtc(true)
        .with_rtr(!fc_on)
        .with_dv(true);

    let msc = frame::Uih {
        id: 0,
        information: Information::ModemStatusCommand(frame::ModemStatusCommand {
            cr: frame::CR::Command,
            dlci: (channel_id + 1) as u8,
            control,
            brk: None,
        }),
    };

    let mut w = port_w.lock().await;
    msc.write(&mut *w).await
}

async fn rx_loop<
    const N: usize,
    const BUF: usize,
    R: embedded_io_async::BufRead,
    W: embedded_io_async::Write,
>(
    port_r: &mut R,
    rx: &mut [FramedProducer<&'_ RxQueue<BUF>, u16>; N],
    lines: &[Lines; N],
    control_channel_opened: &Cell<bool>,
    control_pending_command: &Cell<Option<FrameType>>,
    port_w: &Mutex<NoopRawMutex, W>,
    fc_changed: &Signal<NoopRawMutex, ()>,
) -> Result<(), Error> {
    let mut consecutive_errors = 0u32;
    const MAX_CONSECUTIVE_ERRORS: u32 = 10;

    loop {
        // RxHeader::read is now NEVER cancelled by TX — fixing the cancel-safety bug
        let header_result = frame::RxHeader::read(port_r).await;

        match header_result {
            Err(e) => {
                consecutive_errors += 1;
                error!(
                    "CMUX Runner: RX error: {:?}. Consecutive errors: {}/{}",
                    e, consecutive_errors, MAX_CONSECUTIVE_ERRORS
                );

                if consecutive_errors >= MAX_CONSECUTIVE_ERRORS {
                    error!("Too many consecutive frame errors! Stream is likely corrupted. Terminating CMUX.");
                    return Err(e);
                }
                continue;
            }
            Ok(mut header) => {
                consecutive_errors = 0;

                match header.frame_type {
                    FrameType::Ui | FrameType::Uih if header.is_control() => {
                        let info = match header.read_information().await {
                            Ok(info) => info,
                            Err(e) => {
                                consecutive_errors += 1;
                                error!("Failed to read information from control frame: {:?}. Consecutive errors: {}/{}",
                                        e, consecutive_errors, MAX_CONSECUTIVE_ERRORS);

                                if consecutive_errors >= MAX_CONSECUTIVE_ERRORS {
                                    error!("Too many consecutive frame errors! Terminating CMUX.");
                                    return Err(e);
                                }

                                if let Err(fin_err) = header.finalize().await {
                                    error!(
                                        "Failed to finalize after info read error: {:?}",
                                        fin_err
                                    );
                                }
                                continue;
                            }
                        };

                        if info.is_command() {
                            let mut supported = true;

                            match &info {
                                Information::MultiplexerCloseDown(_cld) => {
                                    // Ack the CLD before returning
                                    {
                                        let mut w = port_w.lock().await;
                                        let _ = info.send_ack(&mut *w).await;
                                    }
                                    if let Err(e) = header.finalize().await {
                                        error!("Failed to finalize CLD frame: {:?}", e);
                                    }
                                    return Err(Error::MultiplexerCloseDown);
                                }
                                Information::TestCommand
                                | Information::PowerSavingControl
                                | Information::RemotePortNegotiationCommand
                                | Information::ServiceNegotiationCommand => {
                                    supported = false;
                                }
                                Information::ModemStatusCommand(msc) => {
                                    let channel_idx = msc.dlci as usize - 1;
                                    if channel_idx >= N {
                                        warn!(
                                                "Modem Status Command for invalid DLCI {}: channel index {} out of bounds (max {})",
                                                msc.dlci, channel_idx, N - 1
                                            );
                                        {
                                            let mut w = port_w.lock().await;
                                            if let Err(e) = info.send_ack(&mut *w).await {
                                                error!("Failed to send ACK for out-of-bounds MSC: {:?}", e);
                                                return Err(e);
                                            }
                                        }
                                        if let Err(e) = header.finalize().await {
                                            error!("Failed to finalize: {:?}", e);
                                        }
                                        continue;
                                    }

                                    let lines = &lines[channel_idx];
                                    let new_control = msc.control.with_ea(false);
                                    let new_brk = msc.brk.map(|b| b.with_ea(false));

                                    let old_fc = lines.rx.get().0.fc();
                                    let new_fc = new_control.fc();
                                    if old_fc != new_fc {
                                        if new_fc {
                                            warn!("Channel {} flow control ENABLED (modem telling us to STOP sending)", msc.dlci);
                                        } else {
                                            info!("Channel {} flow control DISABLED (modem ready to receive)", msc.dlci);
                                        }
                                    }

                                    let old_dv = lines.rx.get().0.dv();
                                    let new_dv = new_control.dv();

                                    if old_dv && !new_dv && consecutive_errors > 0 {
                                        error!("Channel {} data became INVALID (dv: true -> false) after recent errors!", msc.dlci);
                                        error!(
                                            "Channel likely corrupted. CMUX should be restarted!"
                                        );
                                        lines.opened.set(false);
                                        return Err(Error::MalformedFrame);
                                    }

                                    lines.rx.set((new_control, new_brk));

                                    // Signal TX loop if FC changed
                                    if old_fc != new_fc {
                                        fc_changed.signal(());
                                    }
                                }
                                n => {
                                    warn!("Unknown command {:?} for the control channel", n);

                                    {
                                        let mut w = port_w.lock().await;
                                        if let Err(e) = (frame::Uih {
                                            id: 0,
                                            information: Information::NonSupportedCommandResponse(
                                                NonSupportedCommandResponse {
                                                    cr: frame::CR::Response,
                                                    command_type: n.info_type(),
                                                },
                                            ),
                                        })
                                        .write(&mut *w)
                                        .await
                                        {
                                            error!(
                                                "Failed to send NonSupportedCommandResponse: {:?}",
                                                e
                                            );
                                            return Err(e);
                                        }
                                    }

                                    supported = false;
                                }
                            }

                            if supported {
                                let mut w = port_w.lock().await;
                                if let Err(e) = info.send_ack(&mut *w).await {
                                    error!("Failed to send ACK for command: {:?}", e);
                                    return Err(e);
                                }
                            }
                        } else {
                            // received ack for a command
                            if let Information::NonSupportedCommandResponse(
                                NonSupportedCommandResponse { command_type, .. },
                            ) = info
                            {
                                warn!("Mobile station didn't support command: {:?}", command_type);
                            }
                        }
                    }
                    FrameType::Ui | FrameType::Uih => {
                        let channel_id = header.id() as usize - 1;

                        if channel_id >= N {
                            warn!("Received data frame for invalid channel ID {}: index {} out of bounds (max {})",
                                    header.id(), channel_id, N - 1);
                            if let Err(e) = header.finalize().await {
                                error!("Failed to finalize out-of-bounds data frame: {:?}", e);
                                return Err(e);
                            }
                            continue;
                        }

                        let frame_len = header.len;

                        // Try non-blocking grant first. If the bbqueue is full,
                        // send FC=1 to pause the modem on this channel, then
                        // block on wait_grant. This briefly stalls the rx_loop
                        // but preserves the PPP byte stream (no gaps). FC=1
                        // prevents the modem from queuing more data during the stall.
                        let mut grant = match rx[channel_id].grant(frame_len as u16) {
                            Ok(grant) => grant,
                            Err(_) => {
                                warn!(
                                    "Channel {} bbqueue full ({} bytes). Sending FC=1 and waiting for space.",
                                    channel_id + 1,
                                    frame_len
                                );
                                send_msc_fc(port_w, channel_id, true).await?;
                                let grant = rx[channel_id].wait_grant(frame_len as u16).await;
                                info!(
                                    "Channel {} bbqueue space available, sending FC=0 to resume.",
                                    channel_id + 1
                                );
                                send_msc_fc(port_w, channel_id, false).await?;
                                grant
                            }
                        };

                        if let Err(e) = header.copy_to_slice(&mut grant[..frame_len]).await {
                            // Grant is dropped here → auto-aborted, consumer sees nothing
                            drop(grant);
                            consecutive_errors += 1;
                            error!("Failed to copy data to channel {}: {:?}. Consecutive errors: {}/{}",
                                    channel_id + 1, e, consecutive_errors, MAX_CONSECUTIVE_ERRORS);

                            if consecutive_errors >= MAX_CONSECUTIVE_ERRORS {
                                error!("Too many consecutive copy errors! Terminating CMUX.");
                                return Err(e);
                            }

                            warn!("Attempting to skip corrupted frame and resynchronize...");
                            if let Err(fin_err) = header.finalize().await {
                                error!("Failed to finalize after copy error: {:?}", fin_err);
                            }
                            continue;
                        }

                        // FCS is checked in finalize(). If it fails, the grant
                        // is dropped without commit — consumer never sees the data.
                        match header.finalize().await {
                            Ok(()) => {
                                grant.commit(frame_len as u16);
                                trace!("CMUX RX: CH{} {} bytes", channel_id + 1, frame_len);
                            }
                            Err(e) => {
                                // Grant dropped → auto-aborted
                                drop(grant);
                                consecutive_errors += 1;
                                error!("Frame FCS/finalize failed for channel {}: {:?}. Consecutive errors: {}/{}",
                                        channel_id + 1, e, consecutive_errors, MAX_CONSECUTIVE_ERRORS);

                                if consecutive_errors >= MAX_CONSECUTIVE_ERRORS {
                                    error!("Too many consecutive frame errors! Terminating CMUX.");
                                    return Err(e);
                                }
                                continue;
                            }
                        }
                        // Skip the shared finalize at the end — we already handled it.
                        continue;
                    }
                    FrameType::Sabm if header.is_control() => {
                        control_channel_opened.set(true);
                        let mut w = port_w.lock().await;
                        if let Err(e) = (frame::Ua { id: 0 }).write(&mut *w).await {
                            error!("Failed to send UA for control channel SABM: {:?}", e);
                            return Err(e);
                        }
                    }
                    FrameType::Sabm => {
                        let channel_id = header.id() as usize - 1;
                        if channel_id >= N {
                            warn!("Received SABM for invalid channel ID {}: index {} out of bounds (max {})",
                                    header.id(), channel_id, N - 1);
                            let mut w = port_w.lock().await;
                            if let Err(e) = (frame::Dm { id: header.id() }).write(&mut *w).await {
                                error!("Failed to send DM for out-of-bounds SABM: {:?}", e);
                                return Err(e);
                            }
                            if let Err(e) = header.finalize().await {
                                error!("Failed to finalize: {:?}", e);
                            }
                            continue;
                        }
                        lines[channel_id].opened.set(true);
                        fc_changed.signal(());
                        let mut w = port_w.lock().await;
                        if let Err(e) = (frame::Ua { id: header.id() }).write(&mut *w).await {
                            error!(
                                "Failed to send UA for channel {} SABM: {:?}",
                                channel_id + 1,
                                e
                            );
                            return Err(e);
                        }
                    }
                    FrameType::Ua if header.is_control() => {
                        match control_pending_command.get() {
                            Some(FrameType::Sabm) => {
                                control_channel_opened.set(true);
                            }
                            Some(FrameType::Disc) => {
                                control_channel_opened.set(false);
                            }
                            other => {
                                warn!(
                                        "Received UA for control channel with no pending command (was: {:?}), ignoring",
                                        other
                                    );
                            }
                        }
                        control_pending_command.set(None);
                    }
                    FrameType::Ua => {
                        let channel_id = header.id() as usize - 1;
                        if channel_id >= N {
                            warn!("Received UA for invalid channel ID {}: index {} out of bounds (max {})",
                                    header.id(), channel_id, N - 1);
                            if let Err(e) = header.finalize().await {
                                error!("Failed to finalize: {:?}", e);
                            }
                            continue;
                        }
                        match lines[channel_id].pending_command.get() {
                            Some(FrameType::Sabm) => {
                                lines[channel_id].opened.set(true);
                                fc_changed.signal(());
                            }
                            Some(FrameType::Disc) => {
                                lines[channel_id].opened.set(false);
                                fc_changed.signal(());
                            }
                            other => {
                                warn!(
                                        "Received UA for channel {} with no pending command (was: {:?}), ignoring",
                                        channel_id + 1, other
                                    );
                            }
                        }
                        lines[channel_id].pending_command.set(None);
                    }
                    FrameType::Dm if header.is_control() => {
                        error!("Couldn't open control channel - terminating CMUX");
                        if let Err(e) = header.finalize().await {
                            error!("Failed to finalize DM frame: {:?}", e);
                        }
                        return Err(Error::MalformedFrame);
                    }
                    FrameType::Dm => {
                        warn!("Logical channel {} couldn't be opened", header.id() - 1);
                    }
                    FrameType::Disc if header.is_control() => {
                        if control_channel_opened.get() {
                            control_channel_opened.set(false);
                            let mut w = port_w.lock().await;
                            if let Err(e) = (frame::Ua { id: 0 }).write(&mut *w).await {
                                error!("Failed to send UA for control channel DISC: {:?}", e);
                                return Err(e);
                            }
                            if let Err(e) = header.finalize().await {
                                error!("Failed to finalize: {:?}", e);
                            }
                            return Err(Error::MultiplexerCloseDown);
                        } else {
                            let mut w = port_w.lock().await;
                            if let Err(e) = (frame::Dm { id: 0 }).write(&mut *w).await {
                                error!("Failed to send DM for control channel DISC: {:?}", e);
                                return Err(e);
                            }
                        }
                    }
                    FrameType::Disc => {
                        let channel_id = header.id() as usize - 1;
                        if channel_id >= N {
                            warn!("Received DISC for invalid channel ID {}: index {} out of bounds (max {})",
                                    header.id(), channel_id, N - 1);
                            let mut w = port_w.lock().await;
                            if let Err(e) = (frame::Dm { id: header.id() }).write(&mut *w).await {
                                error!("Failed to send DM for out-of-bounds DISC: {:?}", e);
                                return Err(e);
                            }
                            if let Err(e) = header.finalize().await {
                                error!("Failed to finalize: {:?}", e);
                            }
                            continue;
                        }
                        if lines[channel_id].opened.get() {
                            lines[channel_id].opened.set(false);
                            fc_changed.signal(());
                            let mut w = port_w.lock().await;
                            if let Err(e) = (frame::Ua { id: header.id() }).write(&mut *w).await {
                                error!(
                                    "Failed to send UA for channel {} DISC: {:?}",
                                    channel_id + 1,
                                    e
                                );
                                return Err(e);
                            }
                        } else {
                            let mut w = port_w.lock().await;
                            if let Err(e) = (frame::Dm { id: header.id() }).write(&mut *w).await {
                                error!(
                                    "Failed to send DM for channel {} DISC: {:?}",
                                    channel_id + 1,
                                    e
                                );
                                return Err(e);
                            }
                        }
                    }
                }

                if let Err(e) = header.finalize().await {
                    consecutive_errors += 1;
                    error!(
                        "Failed to finalize header: {:?}. Consecutive errors: {}/{}",
                        e, consecutive_errors, MAX_CONSECUTIVE_ERRORS
                    );

                    if consecutive_errors >= MAX_CONSECUTIVE_ERRORS {
                        error!("Too many consecutive frame errors! Stream is corrupted. Terminating CMUX.");
                        return Err(e);
                    }
                    continue;
                }
            }
        }

        embassy_futures::yield_now().await;
    }
}

// rx_loop end

struct MaybeUninitArray<T, const N: usize>(MaybeUninit<[T; N]>);

impl<T, const N: usize> MaybeUninitArray<T, N> {
    fn uninit() -> Self {
        Self(MaybeUninit::uninit())
    }
    unsafe fn set(&mut self, i: usize, val: T) {
        (self.0.as_mut_ptr() as *mut T).add(i).write(val);
    }
    unsafe fn assume_init(self) -> [T; N] {
        self.0.assume_init()
    }
}

impl<'a, const BUF: usize> Channel<'a, BUF> {
    /// Split the channel into RX, TX, and lines handles.
    pub fn split(
        self,
    ) -> (
        ChannelRx<'a, BUF>,
        ChannelTx<'a, BUF>,
        ChannelLines<'a, BUF>,
    ) {
        (
            ChannelRx {
                rx: self.rx,
                lines: self.lines,
                line_status_updated: self.line_status_updated,
            },
            ChannelTx {
                tx: self.tx,
                lines: self.lines,
                line_status_updated: self.line_status_updated,
            },
            ChannelLines {
                lines: self.lines,
                line_status_updated: self.line_status_updated,
            },
        )
    }

    // Note: set_lines, get_lines, set_hangup_detection, clear_hangup_detection
    // are defined below and remain unchanged.

    /// Get a handle to the channel's modem control lines.
    pub fn split_lines(&self) -> ChannelLines<'a, BUF> {
        ChannelLines {
            lines: self.lines,
            line_status_updated: self.line_status_updated,
        }
    }

    /// Set the modem control lines for this channel.
    pub fn set_lines(&self, control: Control, brk: Option<Break>) {
        self.lines.tx.set((control, brk));
        self.line_status_updated.signal(());
    }

    /// Get the current modem control lines for this channel.
    pub fn get_lines(&self) -> (Control, Option<Break>) {
        self.lines.rx.get()
    }

    /// Set hangup detection parameters.
    pub fn set_hangup_detection(&self, mask: u16, val: u16) {
        self.lines.hangup_mask.set(Some((mask, val)));
        self.lines.check_hangup();
    }

    /// Clear hangup detection.
    pub fn clear_hangup_detection(&self) {
        self.lines.hangup_mask.set(None);
        self.lines.check_hangup();
    }
}

impl<'a, const BUF: usize> ChannelRx<'a, BUF> {
    /// Set the modem control lines for this channel.
    pub fn set_lines(&self, control: Control, brk: Option<Break>) {
        self.lines.tx.set((control, brk));
        self.line_status_updated.signal(());
    }

    /// Get the current modem control lines for this channel.
    pub fn get_lines(&self) -> (Control, Option<Break>) {
        self.lines.rx.get()
    }
}

impl<'a, const BUF: usize> ChannelTx<'a, BUF> {
    /// Set the modem control lines for this channel.
    pub fn set_lines(&self, control: Control, brk: Option<Break>) {
        self.lines.tx.set((control, brk));
        self.line_status_updated.signal(());
    }

    /// Get the current modem control lines for this channel.
    pub fn get_lines(&self) -> (Control, Option<Break>) {
        self.lines.rx.get()
    }
}

impl<'a, const BUF: usize> ChannelLines<'a, BUF> {
    /// Set the modem control lines for this channel.
    pub fn set_lines(&self, control: Control, brk: Option<Break>) {
        self.lines.tx.set((control, brk));
        self.line_status_updated.signal(());
    }

    /// Get the current modem control lines for this channel.
    pub fn get_lines(&self) -> (Control, Option<Break>) {
        self.lines.rx.get()
    }
}

/// Error type for channel operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub enum ChannelError {
    /// The channel was hung up by the remote end.
    Hangup,
}

impl core::fmt::Display for ChannelError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        core::write!(f, "{:?}", self)
    }
}

impl core::error::Error for ChannelError {}

impl embedded_io_async::Error for ChannelError {
    fn kind(&self) -> embedded_io_async::ErrorKind {
        match self {
            Self::Hangup => embedded_io_async::ErrorKind::BrokenPipe,
        }
    }
}

impl<'a, const BUF: usize> embedded_io_async::ErrorType for Channel<'a, BUF> {
    type Error = ChannelError;
}

impl<'a, const BUF: usize> embedded_io_async::Read for Channel<'a, BUF> {
    async fn read(&mut self, buf: &mut [u8]) -> Result<usize, Self::Error> {
        check_hangup(self.rx.read(buf), self.lines).await
    }
}

impl<'a, const BUF: usize> embedded_io_async::BufRead for Channel<'a, BUF> {
    async fn fill_buf(&mut self) -> Result<&[u8], Self::Error> {
        check_hangup(self.rx.fill_buf(), self.lines).await
    }

    fn consume(&mut self, amt: usize) {
        self.rx.consume(amt);
    }
}

impl<'a, const BUF: usize> embedded_io_async::Write for Channel<'a, BUF> {
    async fn write(&mut self, buf: &[u8]) -> Result<usize, Self::Error> {
        check_hangup(self.tx.write(buf), self.lines).await
    }

    async fn flush(&mut self) -> Result<(), Self::Error> {
        self.tx.flush().await;
        Ok(())
    }
}

impl<'a, const BUF: usize> embedded_io_async::ErrorType for ChannelRx<'a, BUF> {
    type Error = ChannelError;
}

impl<'a, const BUF: usize> embedded_io_async::Read for ChannelRx<'a, BUF> {
    async fn read(&mut self, buf: &mut [u8]) -> Result<usize, Self::Error> {
        check_hangup(self.rx.read(buf), self.lines).await
    }
}

impl<'a, const BUF: usize> embedded_io_async::BufRead for ChannelRx<'a, BUF> {
    async fn fill_buf(&mut self) -> Result<&[u8], Self::Error> {
        check_hangup(self.rx.fill_buf(), self.lines).await
    }

    fn consume(&mut self, amt: usize) {
        self.rx.consume(amt);
    }
}

impl<'a, const BUF: usize> embedded_io_async::ErrorType for ChannelTx<'a, BUF> {
    type Error = ChannelError;
}

impl<'a, const BUF: usize> embedded_io_async::Write for ChannelTx<'a, BUF> {
    async fn write(&mut self, buf: &[u8]) -> Result<usize, Self::Error> {
        check_hangup(self.tx.write(buf), self.lines).await
    }

    async fn flush(&mut self) -> Result<(), Self::Error> {
        self.tx.flush().await;
        Ok(())
    }
}

fn wait_for_hangup(lines: &Lines) -> impl Future<Output = ()> + '_ {
    poll_fn(|cx| {
        if lines.hangup.get() {
            Poll::Ready(())
        } else {
            lines.hangup_waker.register(cx.waker());
            Poll::Pending
        }
    })
}

async fn check_hangup<'a, F, R>(fut: F, lines: &'a Lines) -> Result<R, ChannelError>
where
    F: Future<Output = R> + 'a,
{
    match select(fut, wait_for_hangup(lines)).await {
        Either::First(r) => Ok(r),
        Either::Second(()) => Err(ChannelError::Hangup),
    }
}

#[must_use = "to delay the drop handler invocation to the end of the scope"]
struct OnDrop<F: FnOnce()> {
    f: core::mem::MaybeUninit<F>,
}

impl<F: FnOnce()> OnDrop<F> {
    fn new(f: F) -> Self {
        Self {
            f: core::mem::MaybeUninit::new(f),
        }
    }

    fn defuse(self) {
        core::mem::forget(self)
    }
}

impl<F: FnOnce()> Drop for OnDrop<F> {
    fn drop(&mut self) {
        unsafe { self.f.as_ptr().read()() }
    }
}
