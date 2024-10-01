use modules_test::{submodules_test::test2, test1};

mod other;
mod subfolder;

mod modules_test{
    pub mod test1;
    pub mod submodules_test{
        pub mod test2;
    }
}

fn main() {
    other::test();
    println!("{:?}",subfolder::get_rng());
    test1::first_test();
    test2::second_test();
}
