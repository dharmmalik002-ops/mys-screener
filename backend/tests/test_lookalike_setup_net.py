"""The retrained setup net's weights load under either transformers naming."""

import unittest

from app.services.lookalike.setup_net import _match_attention_names

OLD = ["attention.attention.query.weight", "attention.attention.key.bias",
       "attention.attention.value.weight", "attention.output.dense.bias", "mlp.fc1.weight"]
NEW = ["attention.q_proj.weight", "attention.k_proj.bias",
       "attention.v_proj.weight", "attention.o_proj.bias", "mlp.fc1.weight"]


class AttentionNameTests(unittest.TestCase):
    def test_old_saved_names_load_into_transformers_5(self):
        # transformers 5 renamed DINOv2's attention; the committed weights use the old names.
        out = _match_attention_names({k: i for i, k in enumerate(OLD)}, set(NEW))
        self.assertEqual(out, {k: i for i, k in enumerate(NEW)})

    def test_names_already_matching_are_left_alone(self):
        saved = {k: i for i, k in enumerate(OLD)}
        self.assertEqual(_match_attention_names(saved, set(OLD)), saved)

    def test_new_names_load_into_older_transformers(self):
        out = _match_attention_names({k: i for i, k in enumerate(NEW)}, set(OLD))
        self.assertEqual(out, {k: i for i, k in enumerate(OLD)})


if __name__ == "__main__":
    unittest.main()
