import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

from transformers import BertForMultipleChoice
import torch
import torch.nn as nn


class BertForPhraseRelevance(BertForMultipleChoice):
    def __init__(self, config):
        super().__init__(config)
        self.num_labels = 2

        # Freeze BERT embeddings
        #for param in self.bert.parameters():
        for param in self.bert.embeddings.parameters():
            param.requires_grad = False

        # Custom classifier for yes/no relevance
        self.classifier = nn.Linear(config.hidden_size, 2) # binary classification (yes/no)
        

    def forward(self, input_ids, attention_mask=None, token_type_ids=None, position_ids=None, 
                head_mask=None, inputs_embeds=None, labels=None):
        
        # Get the outputs from the BERT model
        outputs = self.bert(input_ids, 
                            attention_mask=attention_mask, 
                            token_type_ids=token_type_ids, 
                            position_ids=position_ids, 
                            head_mask=head_mask, 
                            inputs_embeds=inputs_embeds)

        # Use the [CLS] token representation for classification
        #cls_output = outputs.last_hidden_state[:, 0, :]
        # Use the pooled output for classification
        pooled_output = outputs[1]

        # Pass the [CLS] representation through the classifier
        #logits = self.classifier(cls_output)
        logits = self.classifier(pooled_output)

        # Calculate loss if labels are provided
        loss = None
        if labels is not None:
            loss_fct = nn.CrossEntropyLoss()
            loss = loss_fct(logits.view(-1, self.num_labels), labels.view(-1))

        return loss, logits if loss is not None else logits



