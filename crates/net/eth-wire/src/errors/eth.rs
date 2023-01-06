//! Error handling for (`EthStream`)[crate::EthStream]
use crate::{errors::P2PStreamError, DisconnectReason};
use reth_primitives::{Chain, ValidationError, H256};
use std::io;

/// Errors when sending/receiving messages
#[derive(thiserror::Error, Debug)]
#[allow(missing_docs)]
pub enum EthStreamError {
    #[error(transparent)]
    P2PStreamError(#[from] P2PStreamError),
    #[error(transparent)]
    EthHandshakeError(#[from] EthHandshakeError),
    #[error("message size ({0}) exceeds max length (10MB)")]
    MessageTooBig(usize),
}

// === impl EthStreamError ===

impl EthStreamError {
    /// Returns the [`DisconnectReason`] if the error is a disconnect message
    pub fn as_disconnected(&self) -> Option<DisconnectReason> {
        if let EthStreamError::P2PStreamError(err) = self {
            err.as_disconnected()
        } else {
            None
        }
    }

    /// Returns the [io::Error] if it was caused by IO
    pub fn as_io(&self) -> Option<&io::Error> {
        if let EthStreamError::P2PStreamError(P2PStreamError::Io(io)) = self {
            return Some(io)
        }
        None
    }
}

impl From<io::Error> for EthStreamError {
    fn from(err: io::Error) -> Self {
        P2PStreamError::from(err).into()
    }
}

impl From<reth_rlp::DecodeError> for EthStreamError {
    fn from(err: reth_rlp::DecodeError) -> Self {
        P2PStreamError::from(err).into()
    }
}

/// Error variants that can occur during the `eth` sub-protocol handshake.
#[derive(thiserror::Error, Debug)]
#[allow(missing_docs)]
pub enum EthHandshakeError {
    #[error("status message can only be recv/sent in handshake")]
    StatusNotInHandshake,
    #[error("received non-status message when trying to handshake")]
    NonStatusMessageInHandshake,
    #[error("no response received when sending out handshake")]
    NoResponse,
    #[error(transparent)]
    InvalidFork(#[from] ValidationError),
    #[error("mismatched genesis in Status message. expected: {expected:?}, got: {got:?}")]
    MismatchedGenesis { expected: H256, got: H256 },
    #[error("mismatched protocol version in Status message. expected: {expected:?}, got: {got:?}")]
    MismatchedProtocolVersion { expected: u8, got: u8 },
    #[error("mismatched chain in Status message. expected: {expected:?}, got: {got:?}")]
    MismatchedChain { expected: Chain, got: Chain },
}
