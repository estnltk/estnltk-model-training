from pipelines.step01_create_training_corpus.textprocessing.cda_data_cleaning.estnltk_patches.taggers.text_segments_tagger import TextSegmentsTagger
from estnltk.taggers import Tagger


class SubcategorySegmentsTagger(Tagger):
    """Tags subcategory segments

    """

    conf_param = ["tagger", "decorator", "validator", "include_header"]

    def __init__(
        self,
        input_layer: str = "subcategories",
        output_layer: str = "subcategory_segments",
        include_header: bool = False,
        output_attributes=["subcategory"],
    ):
        self.input_layers = [input_layer]

        self.output_attributes = output_attributes
        if output_attributes is None:
            self.output_attributes = ()
        self.output_layer = output_layer
        self.include_header = include_header

        def decorator(span):
            return {"subcategory": span.text}

        self.tagger = TextSegmentsTagger(
            input_layer="subcategories",
            output_attributes=["subcategory"],
            output_layer="subcategory_segments",
            decorator=decorator,
        )

    def _make_layer(self, text, layers, status):
        return self.tagger.make_layer(text=text, layers=layers, status=status)
