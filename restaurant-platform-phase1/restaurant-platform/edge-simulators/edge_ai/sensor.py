"""
Simulated sensing hardware for the plate-waste node: a load cell under the
bussing tray plus a downward-looking camera, and an ambient-light reading.

This is the node's raw signal. The simulator knows the true waste in grams
(it draws it); the node itself never sees that number. It sees four noisy
channels and must infer the grams from them, which is what the on-node model
does (edge_ai/model.py). Keeping the generative process in this one module,
and using it for both the simulator and the offline trainer, is what stops
the two from drifting apart.

The channels are a designed stand-in, not measurements of real hardware:

  scale_g       Net load-cell weight. Unbiased apart from small noise, EXCEPT
                that on one plate in four, cutlery or a napkin was left on
                the tray and adds 15-60 g of weight that is not food. A
                reading from the scale alone is therefore often wrong.
  area_frac     Fraction of the plate the camera sees covered by residue.
                Saturates as the pile grows, so it is poor at high weights.
  height_mm     Mean residue height from a depth estimate. Roughly linear in
                weight, but noisy.
  lux           Ambient light at the station. Dim light makes both camera
                channels noisier, so how much to trust them depends on it.

`lens_fouling` (0 = clean, 1 = badly fouled) is the one fault knob: a dirty
lens dims the light the camera gets and attenuates what it sees, so the
camera channels stop agreeing with the scale in a way the model never saw in
training. That is the situation the node's out-of-distribution guard exists
to notice.

`rng` is anything with random()/gauss()/uniform(): the stdlib `random` module
itself (the simulator) or a random.Random(seed) (the trainer and the tests).
"""

import math

FEATURE_NAMES = ("scale_g", "area_frac", "height_mm", "lux")

# One plate in four carries non-food mass on the tray.
CLUTTER_PROBABILITY = 0.25
CLUTTER_RANGE_G = (15.0, 60.0)


def true_waste_grams(rng, to_go: bool) -> float:
    """
    The ground truth the simulator holds back from the node. This is the
    same process the plate-waste simulator used before the node had a model
    (mean 120 g, sd 45 g, a to-go box cuts what is left to a quarter), kept
    identical so the confounder structure the causal engine is meant to find
    is unchanged.
    """
    base = rng.gauss(120.0, 45.0)
    if to_go:
        base *= 0.25
    return max(0.0, round(base, 1))


def read_sensors(grams: float, rng, lens_fouling: float = 0.0) -> dict:
    """The four raw channel readings for a plate that truly holds `grams`."""
    fouling = min(max(lens_fouling, 0.0), 1.0)

    lux = min(max(rng.gauss(450.0, 90.0), 50.0), 900.0) * (1.0 - 0.5 * fouling)
    darkness = 1.0 - min(lux / 450.0, 1.0)  # 0 in normal light, grows as it dims

    scale = grams + rng.gauss(0.0, 6.0)
    if rng.random() < CLUTTER_PROBABILITY:
        scale += rng.uniform(*CLUTTER_RANGE_G)

    area = 1.0 - math.exp(-grams / 140.0) + rng.gauss(0.0, 0.02 + 0.10 * darkness)
    area = min(max(area, 0.0), 1.0) * (1.0 - 0.6 * fouling)

    height = max(0.0, 1.5 + grams / 45.0 + rng.gauss(0.0, 0.6 + 1.5 * darkness)) * (1.0 - 0.5 * fouling)

    return {"scale_g": scale, "area_frac": area, "height_mm": height, "lux": lux}


def feature_vector(reading: dict) -> list:
    return [reading[name] for name in FEATURE_NAMES]
