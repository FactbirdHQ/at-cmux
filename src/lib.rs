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

use core::cell::{Cell, RefCell};
use core::future::{poll_fn, Future};
use core::mem::MaybeUninit;
use core::pin::pin;
use core::task::Poll;

use embassy_futures::select::{select, select_slice, Either};
use embassy_sync::blocking_mutex::raw::NoopRawMutex;
use embassy_sync::pipe::{Pipe, Reader, Writer};
use embassy_sync::signal::Signal;
use embassy_sync::waitqueue::AtomicWaker;
pub use frame::{Break, Control};
use frame::{Frame, Information, MultiplexerCloseDown};
use futures::FutureExt;
use heapless::Vec;

use crate::frame::{Error, FrameType, NonSupportedCommandResponse};

struct Lines {
    rx: Cell<(Control, Option<Break>)>,
    tx: Cell<(Control, Option<Break>)>,
    opened: Cell<bool>,
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
            hangup: Cell::new(false),
            hangup_mask: Cell::new(None),
            hangup_waker: AtomicWaker::new(),
        }
    }

    fn check_hangup(&self) {
        if !self.rx.get().0.dv() {
            if !self.hangup.get() {
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
}

/// Main CMUX multiplexer.
///
/// Generic parameters:
/// - `N`: Number of logical channels
/// - `BUF`: Buffer size for each channel's TX/RX pipes
pub struct Mux<const N: usize, const BUF: usize> {
    tx: [Pipe<NoopRawMutex, BUF>; N],
    rx: [Pipe<NoopRawMutex, BUF>; N],
    lines: [Lines; N],
    line_status_updated: Signal<NoopRawMutex, ()>,
}

/// A logical CMUX channel for reading and writing data.
pub struct Channel<'a, const BUF: usize> {
    rx: Reader<'a, NoopRawMutex, BUF>,
    tx: Writer<'a, NoopRawMutex, BUF>,
    lines: &'a Lines,
    line_status_updated: &'a Signal<NoopRawMutex, ()>,
}

/// Receive half of a CMUX channel.
pub struct ChannelRx<'a, const BUF: usize> {
    rx: Reader<'a, NoopRawMutex, BUF>,
    lines: &'a Lines,
    line_status_updated: &'a Signal<NoopRawMutex, ()>,
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
    rx: [Writer<'a, NoopRawMutex, BUF>; N],
    control_channel_opened: RefCell<bool>,
    lines: &'a [Lines; N],
    line_status_updated: &'a Signal<NoopRawMutex, ()>,
}

impl<const N: usize, const BUF: usize> Mux<N, BUF> {
    const ONE_PIPE: Pipe<NoopRawMutex, BUF> = Pipe::new();

    /// Create a new CMUX multiplexer.
    pub const fn new() -> Self {
        const LINE: Lines = Lines::new();

        Self {
            rx: [Self::ONE_PIPE; N],
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

        for (i, (tx, rx)) in self.tx.iter_mut().zip(self.rx.iter_mut()).enumerate() {
            let (rx_r, rx_w) = rx.split();
            let (tx_r, tx_w) = tx.split();
            let ch = Channel {
                rx: rx_r,
                tx: tx_w,
                lines: &self.lines[i],
                line_status_updated: &self.line_status_updated,
            };
            unsafe {
                chs.set(i, ch);
                runner_rx.set(i, rx_w);
                runner_tx.set(i, tx_r);
            }
        }
        let runner = Runner {
            rx: unsafe { runner_rx.assume_init() },
            tx: unsafe { runner_tx.assume_init() },
            control_channel_opened: RefCell::new(false),
            lines: &self.lines,
            line_status_updated: &self.line_status_updated,
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
        mut port_w: W,
        max_frame_size: usize,
    ) -> Result<(), Error> {
        let drop_mux = OnDrop::new(|| {
            *self.control_channel_opened.borrow_mut() = false;

            for line in self.lines.iter() {
                line.opened.set(false);
                line.hangup.set(false);
            }
        });

        if !*self.control_channel_opened.borrow() {
            // Send open channel request
            if let Err(e) = (frame::Sabm { id: 0 }).write(&mut port_w).await {
                error!("Failed to send SABM for control channel: {:?}", e);
                return Err(e);
            }
        }

        for channel_id in 0..N {
            if !self.lines[channel_id].opened.get() {
                // Send open channel request
                if let Err(e) = (frame::Sabm {
                    id: channel_id as u8 + 1,
                })
                .write(&mut port_w)
                .await
                {
                    error!(
                        "Failed to send SABM for channel {}: {:?}",
                        channel_id + 1,
                        e
                    );
                    return Err(e);
                }
            }
        }

        // Track consecutive frame errors to detect completely corrupted streams
        let mut consecutive_errors = 0u32;
        const MAX_CONSECUTIVE_ERRORS: u32 = 10;

        loop {
            let mut futs: Vec<_, N> = Vec::new();
            for c in &mut self.tx {
                let res = futs.push(c.fill_buf());
                assert!(res.is_ok());
            }

            match select(
                select_slice(pin!(&mut futs)),
                frame::RxHeader::read(&mut port_r),
            )
            .await
            {
                Either::First((buf, i)) => {
                    if !self.lines[i].opened.get() {
                        let len = buf.len().min(max_frame_size);
                        warn!("Channel {} not opened, dropping {} bytes", i + 1, len);
                        drop(futs);
                        self.tx[i].consume(len);
                        continue;
                    }

                    let len = buf.len().min(max_frame_size);
                    let frame = frame::Uih {
                        id: i as u8 + 1,
                        information: Information::Data(&buf[..len]),
                    };

                    if let Err(e) = frame.write(&mut port_w).await {
                        error!(
                            "CMUX Runner: CH{} failed to write UIH frame: {:?}",
                            i + 1,
                            e
                        );
                        return Err(e);
                    }

                    drop(futs);

                    self.tx[i].consume(len);
                }

                Either::Second(Err(e)) => {
                    consecutive_errors += 1;
                    error!(
                        "CMUX Runner: RX error: {:?}. Consecutive errors: {}/{}",
                        e, consecutive_errors, MAX_CONSECUTIVE_ERRORS
                    );

                    if consecutive_errors >= MAX_CONSECUTIVE_ERRORS {
                        error!("Too many consecutive frame errors! Stream is likely corrupted. Terminating CMUX.");
                        return Err(e);
                    }

                    // RxHeader::read() already tries to find FLAG bytes for synchronization,
                    // so we can just continue and try again
                    continue;
                }
                Either::Second(Ok(mut header)) => {
                    // Successfully read a header, reset error counter
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
                                        error!(
                                            "Too many consecutive frame errors! Terminating CMUX."
                                        );
                                        return Err(e);
                                    }

                                    // Try to finalize to resynchronize, then continue
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
                                        break;
                                    }
                                    Information::TestCommand => {}
                                    Information::ModemStatusCommand(msc) => {
                                        // Validate DLCI is within bounds
                                        let channel_idx = msc.dlci as usize - 1;
                                        if channel_idx >= N {
                                            warn!(
                                                "Modem Status Command for invalid DLCI {}: channel index {} out of bounds (max {})",
                                                msc.dlci, channel_idx, N - 1
                                            );
                                            // Still need to acknowledge the command to avoid protocol errors
                                            if let Err(e) = info.send_ack(&mut port_w).await {
                                                error!("Failed to send ACK for out-of-bounds MSC: {:?}", e);
                                                return Err(e);
                                            }
                                            continue;
                                        }

                                        let lines = &self.lines[channel_idx];
                                        let new_control = msc.control.with_ea(false);
                                        let new_brk = msc.brk.map(|b| b.with_ea(false));

                                        // Check if flow control changed
                                        let old_fc = lines.rx.get().0.fc();
                                        let new_fc = new_control.fc();
                                        if old_fc != new_fc {
                                            if new_fc {
                                                warn!("Channel {} flow control ENABLED (modem telling us to STOP sending)", msc.dlci);
                                            } else {
                                                info!("Channel {} flow control DISABLED (modem ready to receive)", msc.dlci);
                                            }
                                        }

                                        // Check if data valid flag changed - critical for detecting broken channels!
                                        let old_dv = lines.rx.get().0.dv();
                                        let new_dv = new_control.dv();

                                        // Only treat dv: true->false as error if we have recent errors
                                        // (dv=false during normal channel open is expected)
                                        if old_dv && !new_dv && consecutive_errors > 0 {
                                            error!("Channel {} data became INVALID (dv: true -> false) after recent errors!", msc.dlci);
                                            error!("Channel likely corrupted. CMUX should be restarted!");
                                            // Mark channel as closed so upper layers know it's broken
                                            lines.opened.set(false);
                                            // Return error to force CMUX restart
                                            return Err(Error::MalformedFrame);
                                        }

                                        // Update stored modem status
                                        lines.rx.set((new_control, new_brk));
                                    }
                                    n => {
                                        warn!("Unknown command {:?} for the control channel", n);

                                        // Send `InformationType::NonSupportedCommandResponse`
                                        if let Err(e) = (frame::Uih {
                                            id: 0,
                                            information: Information::NonSupportedCommandResponse(
                                                NonSupportedCommandResponse {
                                                    cr: frame::CR::Response,
                                                    command_type: n.info_type(),
                                                },
                                            ),
                                        })
                                        .write(&mut port_w)
                                        .await
                                        {
                                            error!(
                                                "Failed to send NonSupportedCommandResponse: {:?}",
                                                e
                                            );
                                            return Err(e);
                                        }

                                        supported = false;
                                    }
                                }

                                if supported {
                                    // acknowledge the command
                                    if let Err(e) = info.send_ack(&mut port_w).await {
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
                                    warn!(
                                        "Mobile station didn't support command: {:?}",
                                        command_type
                                    );
                                }
                            }
                        }
                        FrameType::Ui | FrameType::Uih => {
                            // data from logical channel
                            let channel_id = header.id() as usize - 1;

                            if channel_id >= N {
                                warn!("Received data frame for invalid channel ID {}: index {} out of bounds (max {})",
                                    header.id(), channel_id, N - 1);
                                // Discard the frame data
                                if let Err(e) = header.finalize().await {
                                    error!("Failed to finalize out-of-bounds data frame: {:?}", e);
                                    return Err(e);
                                }
                                continue;
                            }

                            if let Err(e) = header.copy(&mut self.rx[channel_id]).await {
                                consecutive_errors += 1;
                                error!("Failed to copy data to channel {}: {:?}. Consecutive errors: {}/{}",
                                    channel_id + 1, e, consecutive_errors, MAX_CONSECUTIVE_ERRORS);

                                if consecutive_errors >= MAX_CONSECUTIVE_ERRORS {
                                    error!("Too many consecutive copy errors! Terminating CMUX.");
                                    return Err(e);
                                }

                                // Copy failed mid-frame. Try to skip to next frame by finalizing
                                // (which will attempt resynchronization)
                                warn!("Attempting to skip corrupted frame and resynchronize...");
                                if let Err(fin_err) = header.finalize().await {
                                    error!("Failed to finalize after copy error: {:?}", fin_err);
                                }
                                continue;
                            }
                        }
                        FrameType::Sabm if header.is_control() => {
                            *self.control_channel_opened.borrow_mut() = true;
                            if let Err(e) = (frame::Ua { id: 0 }).write(&mut port_w).await {
                                error!("Failed to send UA for control channel SABM: {:?}", e);
                                return Err(e);
                            }
                        }
                        FrameType::Sabm => {
                            let channel_id = header.id() as usize - 1;
                            if channel_id >= N {
                                warn!("Received SABM for invalid channel ID {}: index {} out of bounds (max {})",
                                    header.id(), channel_id, N - 1);
                                // Respond with DM (Disconnected Mode) to reject the channel
                                if let Err(e) =
                                    (frame::Dm { id: header.id() }).write(&mut port_w).await
                                {
                                    error!("Failed to send DM for out-of-bounds SABM: {:?}", e);
                                    return Err(e);
                                }
                                continue;
                            }
                            self.lines[channel_id].opened.set(true);
                            if let Err(e) = (frame::Ua { id: header.id() }).write(&mut port_w).await
                            {
                                error!(
                                    "Failed to send UA for channel {} SABM: {:?}",
                                    channel_id + 1,
                                    e
                                );
                                return Err(e);
                            }
                        }
                        FrameType::Ua if header.is_control() => {
                            if *self.control_channel_opened.borrow() {
                                *self.control_channel_opened.borrow_mut() = false;
                            } else {
                                *self.control_channel_opened.borrow_mut() = true;
                            }
                        }
                        FrameType::Ua => {
                            let channel_id = header.id() as usize - 1;
                            if channel_id >= N {
                                warn!("Received UA for invalid channel ID {}: index {} out of bounds (max {})",
                                    header.id(), channel_id, N - 1);
                                continue;
                            }
                            if self.lines[channel_id].opened.get() {
                                self.lines[channel_id].opened.set(false);
                            } else {
                                self.lines[channel_id].opened.set(true);
                            }
                        }
                        FrameType::Dm if header.is_control() => {
                            error!("Couldn't open control channel - terminating CMUX");
                            break;
                        }
                        FrameType::Dm => {
                            warn!("Logical channel {} couldn't be opened", header.id() - 1);
                        }
                        FrameType::Disc if header.is_control() => {
                            if *self.control_channel_opened.borrow() {
                                *self.control_channel_opened.borrow_mut() = false;
                                if let Err(e) = (frame::Ua { id: 0 }).write(&mut port_w).await {
                                    error!("Failed to send UA for control channel DISC: {:?}", e);
                                    return Err(e);
                                }
                                break;
                            } else {
                                if let Err(e) = (frame::Dm { id: 0 }).write(&mut port_w).await {
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
                                // Respond with DM to acknowledge the disconnect
                                if let Err(e) =
                                    (frame::Dm { id: header.id() }).write(&mut port_w).await
                                {
                                    error!("Failed to send DM for out-of-bounds DISC: {:?}", e);
                                    return Err(e);
                                }
                                continue;
                            }
                            if self.lines[channel_id].opened.get() {
                                self.lines[channel_id].opened.set(false);
                                if let Err(e) =
                                    (frame::Ua { id: header.id() }).write(&mut port_w).await
                                {
                                    error!(
                                        "Failed to send UA for channel {} DISC: {:?}",
                                        channel_id + 1,
                                        e
                                    );
                                    return Err(e);
                                }
                            } else {
                                if let Err(e) =
                                    (frame::Dm { id: header.id() }).write(&mut port_w).await
                                {
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

                        // Don't terminate yet - the finalize() function has already attempted
                        // to resynchronize the stream. Continue to try reading the next frame.
                        continue;
                    }
                }
            }

            embassy_futures::yield_now().await;
        }

        for id in (0..N).rev() {
            let dlci = id + 1;
            let line = &self.lines[id];
            if line.opened.get() {
                if let Err(e) = (frame::Disc { id: dlci as u8 }).write(&mut port_w).await {
                    error!("Failed to send DISC for channel {}: {:?}", dlci, e);
                    return Err(e);
                }
            }
        }

        if *self.control_channel_opened.borrow() {
            if let Err(e) = (frame::Uih {
                id: 0,
                information: Information::MultiplexerCloseDown(MultiplexerCloseDown {
                    cr: frame::CR::Command,
                }),
            })
            .write(&mut port_w)
            .await
            {
                error!("Failed to send MultiplexerCloseDown: {:?}", e);
                return Err(e);
            }
        }

        drop_mux.defuse();
        Ok(())
    }
}

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
        self.rx.consume(amt)
    }
}

impl<'a, const BUF: usize> embedded_io_async::Write for Channel<'a, BUF> {
    async fn write(&mut self, buf: &[u8]) -> Result<usize, Self::Error> {
        check_hangup(self.tx.write(buf), self.lines).await
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
        self.rx.consume(amt)
    }
}

impl<'a, const BUF: usize> embedded_io_async::ErrorType for ChannelTx<'a, BUF> {
    type Error = ChannelError;
}

impl<'a, const BUF: usize> embedded_io_async::Write for ChannelTx<'a, BUF> {
    async fn write(&mut self, buf: &[u8]) -> Result<usize, Self::Error> {
        check_hangup(self.tx.write(buf), self.lines).await
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

fn check_hangup<'a, F, R>(
    fut: F,
    lines: &'a Lines,
) -> impl Future<Output = Result<R, ChannelError>> + 'a
where
    F: Future<Output = R> + 'a,
{
    select(fut, wait_for_hangup(lines)).map(|e| match e {
        Either::First(r) => Ok(r),
        Either::Second(()) => Err(ChannelError::Hangup),
    })
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
