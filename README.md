# GMP-100 Real-Footage Gate 1A

This branch runs a strict, duration-proportional codec gate on 60 seconds of the openly licensed *Tears of Steel* film.

The gate compares all available visual bytes invested in the structural AV1 stream against a lower-rate structural AV1 stream plus a deterministic optical-flow temporal residual atlas. The atlas receives no free capacity: its bytes are deducted from the structural stream, and it survives only if the completed candidate is no larger while improving fidelity without material temporal or tail-quality regression.

This is a 1080p SDR photorealistic proxy. It is not the native 4K HDR/PQ gate and does not claim that a 143-minute film under 100 MB is solved.

Source attribution: *Tears of Steel* — (CC) Blender Foundation | mango.blender.org — CC BY 3.0.
