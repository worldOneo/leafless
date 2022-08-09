use std::{
  collections::VecDeque,
  fs,
  io::{self, Read, Seek, Write},
};

use crate::encoding::{Decoder, Encoder};

const BLOCK_SIZE: u64 = 4096;

pub struct DataBlock {
  offset: u64,
  size: u64,
}

impl DataBlock {
  pub fn size(&self) -> u64 {
    return self.size;
  }
}

struct BlockStorageMeta {
  offset: u64,
}

impl BlockStorageMeta {
  pub fn serialize(&self) -> VecDeque<u8> {
    Encoder::encode_u64(self.offset)
  }

  pub fn deserialize(&mut self, data: &mut VecDeque<u8>) {
    self.offset = Decoder::decode_u64(data);
  }
}

pub struct BlockStorage {
  file: fs::File,
  meta: BlockStorageMeta,
}

impl BlockStorage {
  pub fn create(file: fs::File) -> io::Result<BlockStorage> {
    let mut storage = BlockStorage {
      file: file,
      meta: BlockStorageMeta { offset: 1 },
    };
    storage.flushMeta()?;
    Ok(storage)
  }

  pub fn open(file: fs::File) -> io::Result<BlockStorage> {
    let mut storage = BlockStorage {
      file: file,
      meta: BlockStorageMeta { offset: 0 },
    };
    let mut header = storage.readData(0, BLOCK_SIZE)?;
    storage.meta.deserialize(&mut header);
    Ok(storage)
  }

  fn flushMeta(&mut self) -> io::Result<()> {
    self.writeFlush(0, &Vec::from(self.meta.serialize()).as_mut_slice())
  }

  fn writeFlush(&mut self, position: u64, data: &[u8]) -> io::Result<()> {
    self.file.seek(io::SeekFrom::Start(position))?;
    self.file.write_all(data)?;
    self.file.flush()
  }

  fn readData(&mut self, position: u64, max_length: u64) -> io::Result<VecDeque<u8>> {
    self.file.seek(io::SeekFrom::Start(position))?;
    let mut buf = Vec::<u8>::with_capacity(max_length as usize);
    buf.resize(max_length as usize, 0);
    let mut total_read: u64 = 0;
    loop {
      let slice = &mut buf[(total_read as usize)..];
      let read = self.file.read(slice)?;
      total_read += read as u64;
      if read == 0 || total_read == max_length {
        break;
      }
    }
    Ok(buf.into())
  }

  fn claimBlock(&mut self, count: u64) -> io::Result<DataBlock> {
    self.meta.offset += count;
    self.file.set_len(self.meta.offset * BLOCK_SIZE)?;
    self.flushMeta()?;
    Ok(DataBlock {
      offset: self.meta.offset - count,
      size: count * BLOCK_SIZE,
    })
  }

  pub fn writeBlockOffset(
    &mut self,
    block: &DataBlock,
    offset: u64,
    data: VecDeque<u8>,
  ) -> io::Result<()> {
    if data.len() as u64 + offset > block.size {
      Err(io::Error::new(
        io::ErrorKind::InvalidData,
        "Size exceeds block size",
      ))
    } else {
      self.writeFlush(
        block.offset * BLOCK_SIZE + offset,
        Vec::from(data).as_slice(),
      )
    }
  }

  pub fn writeBlock(&mut self, block: &DataBlock, data: VecDeque<u8>) -> io::Result<()> {
    self.writeBlockOffset(block, 0, data)
  }

  pub fn readBlockOffset(
    &mut self,
    block: &DataBlock,
    offset: u64,
    max_length: u64,
  ) -> io::Result<VecDeque<u8>> {
    self.readData(block.offset * BLOCK_SIZE + offset, max_length)
  }

  pub fn readBlock(&mut self, block: &DataBlock) -> io::Result<VecDeque<u8>> {
    self.readBlockOffset(block, 0, block.size)
  }
}

#[cfg(test)]
mod tests {
  use rand::{self, Rng};
  use std::collections::VecDeque;
  use std::env::temp_dir;
  use std::fs;
  use super::BLOCK_SIZE;
  use super::BlockStorage;

  fn create_temp_file_name() -> std::path::PathBuf {
    let temp_file_name: String = rand::thread_rng()
      .sample_iter(&rand::distributions::Alphanumeric)
      .take(16)
      .map(char::from)
      .collect();

    temp_dir().join(temp_file_name + ".leafless")
  }

  fn create_temp_storage() -> BlockStorage {
    let mut options = fs::File::options();
    let open = options.read(true).write(true).create(true);
    BlockStorage::create(open.open(create_temp_file_name()).unwrap()).unwrap()
  }


  #[test]
  fn test_create_block_storage() {
    let file_name = create_temp_file_name();
    let mut options = fs::File::options();
    let open = options.read(true).write(true).create(true);
    BlockStorage::create(open.open(file_name.clone()).unwrap()).unwrap();
    let storage = BlockStorage::open(open.open(file_name).unwrap()).unwrap();
    assert_eq!(storage.meta.offset, 1);
  }

  #[test]
  fn test_claim_storage() {
    let mut storage = create_temp_storage();
    let block = storage.claimBlock(1).unwrap();
    let data = "data"
      .chars()
      .into_iter()
      .map(|c| c as u8)
      .collect::<VecDeque<_>>();
    storage.writeBlock(&block, data.clone()).unwrap();
    let mut read = storage.readBlock(&block).unwrap();
    read.resize(4, 0);
    assert_eq!(read, data);
    read.resize(BLOCK_SIZE as usize + 1, 0);
    assert!(storage.writeBlock(&block, read).is_err());
  }
}
