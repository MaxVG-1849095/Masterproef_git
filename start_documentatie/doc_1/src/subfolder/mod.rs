use rand::Rng;

pub fn get_rng() -> i32{
    let mut rng = rand::thread_rng();
    rng.gen_range(0..10)
}