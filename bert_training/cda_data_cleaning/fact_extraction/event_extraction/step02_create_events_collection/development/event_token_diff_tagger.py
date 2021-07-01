from estnltk.taggers import DiffTagger


class EventTokenDiffTagger(DiffTagger):
    """Tag diff of 'event_tokens' and 'event_tokens_dev' layers.

    """

    def __init__(self, output_layer="event_tokens_diff"):
        super().__init__(
            layer_a="event_tokens",
            layer_b="event_tokens_dev",
            output_layer=output_layer,
            output_attributes=("grammar_symbol", "unit_type", "value", "specialty_code", "specialty", "regex_type"),
        )
