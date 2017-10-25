use byteorder::LittleEndian;
use byteorder::ReadBytesExt;
use byteorder::WriteBytesExt;

use std::mem::size_of;

use std::io::Cursor;
use std::io::Result;
use std::io::Write;

use std::slice::from_raw_parts;

pub struct ReadLeaf<'a> {
    id: u64,
    epoch: u64,
    size: usize,
    #[allow(dead_code)]
    data: &'a [u8],
    keys: Vec<&'a [u8]>,
    vals: Vec<&'a [u8]>,
}

pub fn serialize_readleaf(wtr: &mut Write, leaf: &ReadLeaf) -> Result<usize> {
    let size = leaf.size;
    let mut total = 3 * size_of::<u64>() + 2 * size * size_of::<u64>();
    wtr.write_u64::<LittleEndian>(leaf.id)?;
    wtr.write_u64::<LittleEndian>(leaf.epoch)?;
    wtr.write_u64::<LittleEndian>(leaf.size as u64)?;
    for i in 0..size {
        let len = leaf.keys[i].len();
        total += len;
        wtr.write_u64::<LittleEndian>(len as u64)?;
    }
    for i in 0..size {
        let len = leaf.vals[i].len();
        total += len;
        wtr.write_u64::<LittleEndian>(len as u64)?;
    }
    for i in 0..size {
        wtr.write_all(leaf.keys[i])?;
    }
    for i in 0..size {
        wtr.write_all(leaf.vals[i])?;
    }
    Ok(total)
}

pub fn deserialize_readleaf(input: &[u8]) -> Result<ReadLeaf> {
    let input_ptr = input.as_ptr();
    let mut rdr = Cursor::new(input);
    let id = rdr.read_u64::<LittleEndian>()?;
    let epoch = rdr.read_u64::<LittleEndian>()?;
    let size = rdr.read_u64::<LittleEndian>()? as usize;

    let empty: &[u8] = &[];
    let mut keys = vec![empty; size];
    let mut vals = vec![empty; size];

    let mut offset = (3 * size_of::<u64>() + 2 * size * size_of::<u64>()) as isize;

    for i in 0..size {
        let len = rdr.read_u64::<LittleEndian>()? as usize;
        unsafe {
            keys[i] = from_raw_parts(input_ptr.offset(offset), len);
        }
        offset += len as isize;
    }

    for i in 0..size {
        let len = rdr.read_u64::<LittleEndian>()? as usize;
        unsafe {
            vals[i] = from_raw_parts(input_ptr.offset(offset), len);
        }
        offset += len as isize;
    }

    Ok(ReadLeaf {
        id: id,
        epoch: epoch,
        size: size,
        data: input,
        keys: keys,
        vals: vals,
    })
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
        let input = ReadLeaf{
            id: 0,
            epoch: 0,
            size: 0,
            data: empty,
            keys: vec![],
            vals: vec![],
        };
        let mut wtr = vec![];
        let result = serialize_readleaf(&mut wtr, &input);
        assert!(result.is_ok());
        assert_eq!(3 * size_of::<u64>(), wtr.len());
        let output = deserialize_readleaf(&wtr);
        assert!(output.is_ok());
        let output = output.unwrap();
        assert_eq!(0, output.id);
        assert_eq!(0, output.epoch);
        assert_eq!(0, output.size);
        assert!(output.data != empty);
        assert_eq!(0, output.keys.len());
        assert_eq!(0, output.vals.len());
    }

    #[test]
    fn roundtrip_nonempty_readleaf() {
        let empty: &[u8] = &[];
        let input = ReadLeaf{
            id: 3,
            epoch: 2,
            size: 1,
            data: empty,
            keys: vec![b"hello"],
            vals: vec![b"world"],
        };
        let mut wtr = vec![];
        let result = serialize_readleaf(&mut wtr, &input);
        assert!(result.is_ok());
        assert_eq!(5 * size_of::<u64>() + "hello".len() + "world".len(), wtr.len());
        let output = deserialize_readleaf(&wtr);
        assert!(output.is_ok());
        let output = output.unwrap();
        assert_eq!(3, output.id);
        assert_eq!(2, output.epoch);
        assert_eq!(1, output.size);
        assert_eq!(b"hello", output.keys[0]);
        assert_eq!(b"world", output.vals[0]);
    }

    #[test]
    fn roundtrip_random_readleaf() {
        let mut data = vec![];
        let input = create_random_readleaf(100, &mut data);
        let mut wtr = vec![];
        let result = serialize_readleaf(&mut wtr, &input);
        assert!(result.is_ok());
        let output = deserialize_readleaf(&wtr);
        assert!(output.is_ok());
        let output = output.unwrap();
        assert_eq!(input.id, output.id);
        assert_eq!(output.epoch, output.epoch);
        assert_eq!(100, output.size);
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
        let result = serialize_readleaf(&mut wtr, &input);
        assert!(result.is_ok());
        b.iter(|| deserialize_readleaf(&wtr))
    }

    #[bench]
    fn deserialize_rnode_bench100(b: &mut Bencher) {
        let mut data = vec![];
        let input = create_random_readleaf(100, &mut data);
        let mut wtr = vec![];
        let result = serialize_readleaf(&mut wtr, &input);
        assert!(result.is_ok());
        b.iter(|| deserialize_readleaf(&wtr))
    }

    fn insert_random_bytes(size : usize, data : &mut Vec<u8>, rng : &mut StdRng) -> Vec<usize> {
        let mut lengths = vec![0 as usize; size];
        for i in 0..size {
            let len = rng.gen::<usize>() % 100;
            let key = rng.gen_ascii_chars().take(len).collect::<String>();
            data.extend(key.into_bytes());
            lengths[i] = len;
        }
        lengths
    }

    fn insert_random_arrays(size: usize, offset: isize, ptr: *const u8, data: &mut Vec<&[u8]>, lengths: &Vec<usize>) -> isize {
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

    fn create_random_readleaf(size : usize, data : &mut Vec<u8>) -> ReadLeaf {
        let seed: &[_] = &[1, 2, 3, 4];
        let mut rng: StdRng = SeedableRng::from_seed(seed);
        let id = rng.gen::<u64>();
        let epoch = rng.gen::<u64>();

        let mut offset = 0 as isize;

        let empty: &[u8] = &[];
        let keylen = insert_random_bytes(size, data, &mut rng);
        let vallen = insert_random_bytes(size, data, &mut rng);

        let mut keys = vec![empty; size];
        let mut vals = vec![empty; size];
        let data_ptr = data.as_ptr();

        offset = insert_random_arrays(size, offset, data_ptr, &mut keys, &keylen);
        insert_random_arrays(size, offset, data_ptr, &mut vals, &vallen);

        ReadLeaf{
            id: id,
            epoch: epoch,
            size: size,
            data: data,
            keys: keys,
            vals: vals,
        }
    }
}