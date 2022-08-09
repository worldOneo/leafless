use std::collections::VecDeque;

pub struct Encoder {}
pub struct Decoder {}

const BITS_PER_BYTE: u64 = 7;
const SEVEN_BYTES_ONE_BIT: u64 = 0xFF_FF_FF_FF_FF_FF_FF_00 + 0b1000_0000;
const SEVEN_BITS: u64 = 0b0111_1111;
const HAS_NEXT: u8 = 0b1000_0000;

impl Encoder {
  pub fn encode_u64(mut value: u64) -> VecDeque<u8> {
    let mut buf = VecDeque::<u8>::with_capacity(9);
    for _ in 0..8 {
      let mut byte = (value & SEVEN_BITS) as u8;
      value = value & SEVEN_BYTES_ONE_BIT;
      value >>= BITS_PER_BYTE;
      if value != 0 {
        byte |= HAS_NEXT;
      }
      buf.push_back(byte);
      if value == 0 {
        break;
      }
    }
    if value != 0 {
      let byte: u8 = (value & 0xFF) as u8;
      buf.push_back(byte);
    }

    buf
  }
}

impl Decoder {
  pub fn decode_u64(data: &mut VecDeque<u8>) -> u64 {
    let mut value: u64 = 0;
    for i in 0..8 {
      if let Some(byte) = data.pop_front() {
        let byte_val = byte & (SEVEN_BITS as u8);
        let has_next = (byte & HAS_NEXT) != 0;
        value += (byte_val as u64) << (BITS_PER_BYTE * i);
        if !has_next {
          return value;
        }
      } else {
        return value;
      }
    }
    if let Some(byte) = data.pop_front() {
      value += (byte as u64) << BITS_PER_BYTE * 8;
    }
    value
  }
}

#[cfg(test)]
mod tests {
  use crate::encoding::{Decoder, Encoder};

  #[test]
  fn test_u64_encoding() {
    let cases: Vec<u64> = vec![0, 1, 0xFFFFFFFFFFFFFFFF, 0xFF_00_00];
    for case in cases {
      assert_eq!(Decoder::decode_u64(&mut Encoder::encode_u64(case)), case);
    }
  }
}
