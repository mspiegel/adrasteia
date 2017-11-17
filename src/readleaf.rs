
use std::io::Cursor;
use std::io::Result;
use std::io::Write;
use std::mem::size_of;
use std::slice::from_raw_parts;

use byteorder::LittleEndian;
use byteorder::ReadBytesExt;
use byteorder::WriteBytesExt;

pub struct ReadLeaf<'a> {
    #[allow(dead_code)]
    pub data: &'a [u8],
    pub keys: Vec<&'a [u8]>,
    pub vals: Vec<&'a [u8]>,
}

impl<'a> ReadLeaf<'a> {
    pub fn serialize(&self, wtr: &mut Write) -> Result<usize> {
        let size = self.keys.len();
        let mut total = size_of::<u64>() + 2 * size * size_of::<u64>();
        wtr.write_u64::<LittleEndian>(size as u64)?;
        for key in &self.keys {
            let len = key.len();
            total += len;
            wtr.write_u64::<LittleEndian>(len as u64)?;
        }
        for val in &self.vals {
            let len = val.len();
            total += len;
            wtr.write_u64::<LittleEndian>(len as u64)?;
        }
        for key in &self.keys {
            wtr.write_all(key)?;
        }
        for val in &self.vals {
            wtr.write_all(val)?;
        }
        Ok(total)
    }

    pub fn deserialize(input: &[u8]) -> Result<ReadLeaf> {
        let input_ptr = input.as_ptr();
        let mut rdr = Cursor::new(input);
        let size = rdr.read_u64::<LittleEndian>()? as usize;

        let empty: &[u8] = &[];
        let mut keys = vec![empty; size];
        let mut vals = vec![empty; size];

        let mut offset = (size_of::<u64>() + 2 * size * size_of::<u64>()) as isize;

        for key in &mut keys {
            let len = rdr.read_u64::<LittleEndian>()? as usize;
            unsafe {
                *key = from_raw_parts(input_ptr.offset(offset), len);
            }
            offset += len as isize;
        }

        for val in &mut vals {
            let len = rdr.read_u64::<LittleEndian>()? as usize;
            unsafe {
                *val = from_raw_parts(input_ptr.offset(offset), len);
            }
            offset += len as isize;
        }

        Ok(ReadLeaf {
            data: input,
            keys: keys,
            vals: vals,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use rand::Rng;
    use rand::SeedableRng;
    use rand::StdRng;

    use test::Bencher;

    #[test]
    fn roundtrip_empty_readleaf() {
        let empty: &[u8] = &[];
        let input = ReadLeaf {
            data: empty,
            keys: vec![],
            vals: vec![],
        };
        let mut wtr = vec![];
        let result = input.serialize(&mut wtr);
        assert!(result.is_ok());
        assert_eq!(size_of::<u64>(), wtr.len());
        let output = ReadLeaf::deserialize(&wtr);
        assert!(output.is_ok());
        let output = output.unwrap();
        assert!(output.data != empty);
        assert_eq!(0, output.keys.len());
        assert_eq!(0, output.vals.len());
    }

    #[test]
    fn roundtrip_nonempty_readleaf() {
        let empty: &[u8] = &[];
        let input = ReadLeaf {
            data: empty,
            keys: vec![b"hello"],
            vals: vec![b"world"],
        };
        let mut wtr = vec![];
        let result = input.serialize(&mut wtr);
        assert!(result.is_ok());
        assert_eq!(
            3 * size_of::<u64>() + "hello".len() + "world".len(),
            wtr.len()
        );
        let output = ReadLeaf::deserialize(&wtr);
        assert!(output.is_ok());
        let output = output.unwrap();
        assert_eq!(b"hello", output.keys[0]);
        assert_eq!(b"world", output.vals[0]);
    }

    #[test]
    fn roundtrip_random_readleaf() {
        let mut data = vec![];
        let input = create_random_readleaf(100, &mut data);
        let mut wtr = vec![];
        let result = input.serialize(&mut wtr);
        assert!(result.is_ok());
        let output = ReadLeaf::deserialize(&wtr);
        assert!(output.is_ok());
        let output = output.unwrap();
        for i in 0..100 {
            assert_eq!(input.keys[i], output.keys[i]);
            assert_eq!(input.vals[i], output.vals[i]);
        }
    }

    #[bench]
    fn deserialize_rnode_bench10(b: &mut Bencher) {
        let mut data = vec![];
        let input = create_random_readleaf(10, &mut data);
        let mut wtr = vec![];
        let result = input.serialize(&mut wtr);
        assert!(result.is_ok());
        b.iter(|| ReadLeaf::deserialize(&wtr))
    }

    #[bench]
    fn deserialize_rnode_bench100(b: &mut Bencher) {
        let mut data = vec![];
        let input = create_random_readleaf(100, &mut data);
        let mut wtr = vec![];
        let result = input.serialize(&mut wtr);
        assert!(result.is_ok());
        b.iter(|| ReadLeaf::deserialize(&wtr))
    }

    fn create_bytes(size: usize, data: &mut Vec<u8>, rng: &mut StdRng) -> Vec<usize> {
        let mut lengths = vec![0 as usize; size];
        for i in 0..size {
            let len = rng.gen::<usize>() % 100;
            let key = rng.gen_ascii_chars().take(len).collect::<String>();
            data.extend(key.into_bytes());
            lengths[i] = len;
        }
        lengths
    }

    fn create_arrays(
        size: usize,
        offset: isize,
        ptr: *const u8,
        data: &mut Vec<&[u8]>,
        lengths: &Vec<usize>,
    ) -> isize {
        let mut offset = offset;
        for i in 0..size {
            let len = lengths[i];
            unsafe {
                data[i] = from_raw_parts(ptr.offset(offset), len);
            }
            offset += len as isize;
        }
        offset
    }

    fn create_random_readleaf(size: usize, data: &mut Vec<u8>) -> ReadLeaf {
        let seed: &[_] = &[1, 2, 3, 4];
        let mut rng: StdRng = SeedableRng::from_seed(seed);

        let mut offset = 0 as isize;

        let empty: &[u8] = &[];
        let keylen = create_bytes(size, data, &mut rng);
        let vallen = create_bytes(size, data, &mut rng);

        let mut keys = vec![empty; size];
        let mut vals = vec![empty; size];
        let data_ptr = data.as_ptr();

        offset = create_arrays(size, offset, data_ptr, &mut keys, &keylen);
        create_arrays(size, offset, data_ptr, &mut vals, &vallen);

        ReadLeaf {
            data: data,
            keys: keys,
            vals: vals,
        }
    }
}
