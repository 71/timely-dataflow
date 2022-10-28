use differential_dataflow::input::InputSession;
use differential_dataflow::operators::{Join, Consolidate};
use wasm_bindgen::JsValue;
use wasm_bindgen::prelude::wasm_bindgen;

#[wasm_bindgen]
extern "C" {
    #[wasm_bindgen(js_namespace = console)]
    fn log(value: wasm_bindgen::JsValue);
}

// Sources:
//   https://timelydataflow.github.io/differential-dataflow/chapter_0/chapter_0_1.html
//   https://timelydataflow.github.io/differential-dataflow/chapter_0/chapter_0_2.html
#[wasm_bindgen]
pub fn run(size: usize) -> Result<(), JsValue> {
    // define a new timely dataflow computation.
    timely::execute_directly(move |worker| {

        // create an input collection of data.
        let mut input = InputSession::new();

        // define a new computation.
        worker.dataflow(|scope| {

            // create a new collection from our input.
            let manages = input.to_collection(scope);

            // if (m2, m1) and (m1, p), then output (m1, (m2, p))
            manages
                .map(|(m2, m1)| (m1, m2))
                .join(&manages)
                .consolidate()
                .inspect(|x| log(format!("{x:?}").into()));
        });

        // Load input (a binary tree).
        input.advance_to(0);

        for person in 0..size {
            input.insert((person / 2, person));
        }

        for person in 1..size {
            input.advance_to(person);
            input.remove((person / 2, person));
            input.insert((person / 3, person));
        }
    });

    Ok(())
}

#[test]
fn run_with_deno() {
    assert!(std::env::current_dir().unwrap().ends_with("wasm-example"));

    let result = cmd_lib::run_fun! {
        cargo build --release --target wasm32-unknown-unknown --package wasm-example;
        wasm-bindgen ../target/wasm32-unknown-unknown/release/wasm_example.wasm --out-dir ./wasm --target deno;
        deno eval "import { run } from './wasm/wasm_example.js'; run(10);";
    }.unwrap();

    assert_eq!(result, "\
((0, (0, 0)), 0, 1)
((0, (0, 1)), 0, 1)
((0, (0, 2)), 2, 1)
((1, (0, 2)), 0, 1)
((1, (0, 2)), 2, -1)
((1, (0, 3)), 0, 1)
((1, (0, 4)), 4, 1)
((1, (0, 5)), 5, 1)
((2, (0, 4)), 2, 1)
((2, (0, 4)), 4, -1)
((2, (0, 5)), 2, 1)
((2, (0, 5)), 5, -1)
((2, (0, 6)), 6, 1)
((2, (0, 7)), 7, 1)
((2, (0, 8)), 8, 1)
((2, (1, 4)), 0, 1)
((2, (1, 4)), 2, -1)
((2, (1, 5)), 0, 1)
((2, (1, 5)), 2, -1)
((3, (1, 6)), 0, 1)
((3, (1, 6)), 6, -1)
((3, (1, 7)), 0, 1)
((3, (1, 7)), 7, -1)
((3, (1, 9)), 9, 1)
((4, (1, 8)), 4, 1)
((4, (1, 8)), 8, -1)
((4, (1, 9)), 4, 1)
((4, (1, 9)), 9, -1)
((4, (2, 8)), 0, 1)
((4, (2, 8)), 4, -1)
((4, (2, 9)), 0, 1)
((4, (2, 9)), 4, -1)");
}
