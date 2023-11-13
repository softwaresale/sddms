
fn main() -> Result<(), Box<dyn std::error::Error>> {

    tonic_build::configure()
        .compile(
            &[
                "proto/site_manager.proto",
                "proto/concurrency_controller.proto",
                "proto/finalize_mode.proto",
                "proto/api_result.proto",
            ],
            &[
                "proto/"
            ]
        )?;

    Ok(())
}
