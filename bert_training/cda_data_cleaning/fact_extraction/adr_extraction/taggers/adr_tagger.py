from estnltk.taggers import PhraseTagger
import os.path


class AdrTagger(PhraseTagger):
    """Adverse drag reaction tagger."""

    def __init__(
        self,
        output_layer="adr",
        input_layers=("morph_analysis",),
        vocabulary=os.path.join(os.path.dirname(__file__), "adr_vocab_larger_edited.csv"),
        key="_phrase_",
        output_attributes=("value", "_phrase_"),
        conflict_resolving_strategy="MAX",
    ):
        assert isinstance(input_layers, (list, tuple)), "'input_layers' should be list or tuple of str"

        super().__init__(
            output_layer=output_layer,
            input_layer=input_layers[0],
            input_attribute="lemma",
            vocabulary=vocabulary,
            key=key,
            output_attributes=output_attributes,
            conflict_resolving_strategy=conflict_resolving_strategy,
            output_ambiguous=True,
        )
