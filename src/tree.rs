pub struct WriteTree {
    pub epoch: u64,
    pub id: u64,
    pub max_pivots: usize,
    pub max_buffer: usize,
}

impl WriteTree {
    pub fn new(max_pivots: usize, max_buffer: usize) -> WriteTree {
        WriteTree {
            epoch: 1,
            id: 1,
            max_pivots: max_pivots,
            max_buffer: max_buffer,
        }
    }

    pub fn next_id(&mut self) -> u64 {
        let id = self.id;
        self.id += 1;
        id
    }
}
