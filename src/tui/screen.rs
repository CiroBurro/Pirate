#[derive(Default)]
pub enum Screen {
    #[default]
    Main,
    Detail {
        selected: usize,
    },
    Log,
}
