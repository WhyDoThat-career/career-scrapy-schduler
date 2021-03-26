import tensorflow as tf
import numpy as np
from konlpy.tag import Okt
import pandas as pd
import pickle

converter = Okt()
labels = pd.read_csv('ML/label.csv')

class MultiHeadAttention(tf.keras.layers.Layer):
    def __init__(self, embedding_dim, num_heads=8,** kwargs):
        super(MultiHeadAttention, self).__init__()
        self.embedding_dim = embedding_dim # d_model
        self.num_heads = num_heads

        assert embedding_dim % self.num_heads == 0

        self.projection_dim = embedding_dim // num_heads
        self.query_dense = tf.keras.layers.Dense(embedding_dim)
        self.key_dense = tf.keras.layers.Dense(embedding_dim)
        self.value_dense = tf.keras.layers.Dense(embedding_dim)
        self.dense = tf.keras.layers.Dense(embedding_dim)

    def scaled_dot_product_attention(self, query, key, value):
        matmul_qk = tf.matmul(query, key, transpose_b=True)
        depth = tf.cast(tf.shape(key)[-1], tf.float32)
        logits = matmul_qk / tf.math.sqrt(depth)
        attention_weights = tf.nn.softmax(logits, axis=-1)
        output = tf.matmul(attention_weights, value)
        return output, attention_weights

    def split_heads(self, x, batch_size):
        x = tf.reshape(x, (batch_size, -1, self.num_heads, self.projection_dim))
        return tf.transpose(x, perm=[0, 2, 1, 3])
    
    def get_config(self) :
        config = super().get_config().copy()
        config.update({
            'embedding_dim' : self.embedding_dim,
            'num_heads' :self.num_heads,
            'projection_dim' : self.projection_dim,
            'query_dense' :self.query_dense,
            'key_dense' : self.key_dense,
            'value_dense' :self.value_dense,
            'dense' :self.dense,
        })
        return config

    def call(self, inputs):
        # x.shape = [batch_size, seq_len, embedding_dim]
        batch_size = tf.shape(inputs)[0]

        # (batch_size, seq_len, embedding_dim)
        query = self.query_dense(inputs)
        key = self.key_dense(inputs)
        value = self.value_dense(inputs)

        # (batch_size, num_heads, seq_len, projection_dim)
        query = self.split_heads(query, batch_size)  
        key = self.split_heads(key, batch_size)
        value = self.split_heads(value, batch_size)

        scaled_attention, _ = self.scaled_dot_product_attention(query, key, value)
        # (batch_size, seq_len, num_heads, projection_dim)
        scaled_attention = tf.transpose(scaled_attention, perm=[0, 2, 1, 3])  

        # (batch_size, seq_len, embedding_dim)
        concat_attention = tf.reshape(scaled_attention, (batch_size, -1, self.embedding_dim))
        outputs = self.dense(concat_attention)
        return outputs

class TransformerBlock(tf.keras.layers.Layer):
    def __init__(self, embedding_dim, num_heads, dff, rate=0.1,** kwargs):
        super(TransformerBlock, self).__init__()
        self.att = MultiHeadAttention(embedding_dim, num_heads)
        self.ffn = tf.keras.Sequential(
            [tf.keras.layers.Dense(dff, activation="relu"),
             tf.keras.layers.Dense(embedding_dim),]
        )
        self.layernorm1 = tf.keras.layers.LayerNormalization(epsilon=1e-6)
        self.layernorm2 = tf.keras.layers.LayerNormalization(epsilon=1e-6)
        self.dropout1 = tf.keras.layers.Dropout(rate)
        self.dropout2 = tf.keras.layers.Dropout(rate)
        
    def get_config(self) :
        config = super().get_config().copy()
        config.update({
            'att' : self.att,
            'ffn' :self.ffn,
            'layernorm1' : self.layernorm1,
            'layernorm2' :self.layernorm2,
            'dropout1' : self.dropout1,
            'dropout2' :self.dropout2,
        })
        return config

    def call(self, inputs, training):
        attn_output = self.att(inputs)
        attn_output = self.dropout1(attn_output, training=training)
        out1 = self.layernorm1(inputs + attn_output)
        ffn_output = self.ffn(out1)
        ffn_output = self.dropout2(ffn_output, training=training)
        return self.layernorm2(out1 + ffn_output)

class TokenAndPositionEmbedding(tf.keras.layers.Layer):
    def __init__(self, max_len, vocab_size, embedding_dim,** kwargs):
        super(TokenAndPositionEmbedding, self).__init__()
        self.token_emb = tf.keras.layers.Embedding(vocab_size, embedding_dim)
        self.pos_emb = tf.keras.layers.Embedding(max_len, embedding_dim)
    
    def get_config(self) :
        config = super().get_config().copy()
        config.update({
            'token_emb' : self.token_emb,
            'pos_emb' :self.pos_emb,
        })
        return config

    def call(self, x):
        max_len = tf.shape(x)[-1]
        positions = tf.range(start=0, limit=max_len, delta=1)
        positions = self.pos_emb(positions)
        x = self.token_emb(x)
        return x + positions

class AttentionModel :
    def __init__(self):
        self.tokenizer = self.get_tokenizer()
        self.START_TOKEN, self.END_TOKEN,self.VOCAB_SIZE = self.get_token()
        self.MAX_LENGTH = 20
        self.model = self.get_model()

    def get_tokenizer(self,tokenizer_path='ML/tokenizer.pickle') :
        with open(tokenizer_path, 'rb') as handle:
            tokenizer = pickle.load(handle)
        return tokenizer

    def get_token(self):
        START_TOKEN, END_TOKEN = [self.tokenizer.vocab_size], [self.tokenizer.vocab_size + 1]
        VOCAB_SIZE = self.tokenizer.vocab_size + 2
        # print('시작 토큰 번호 :',START_TOKEN)
        # print('종료 토큰 번호 :',END_TOKEN)
        # print('단어 집합의 크기 :',VOCAB_SIZE)
        return START_TOKEN, END_TOKEN,VOCAB_SIZE

    def proc_eval(self,inputs):
        inputs = [inputs]
        tokenized_inputs = []

        for sentence1 in inputs:
            sentence1 = self.START_TOKEN + self.tokenizer.encode(sentence1) + self.END_TOKEN
            tokenized_inputs.append(sentence1)

        tokenized_inputs = tf.keras.preprocessing.sequence.pad_sequences(
            tokenized_inputs, maxlen=self.MAX_LENGTH, padding='post')

        return tokenized_inputs

    def predict(self,sentence) :
        okt_title = converter.pos(sentence)
        sentence = ' '.join([tup[0].upper() for tup in okt_title if tup[1] == 'Noun' or tup[1] == 'Alpha'])
        pred = self.model.predict(self.proc_eval(sentence))
        return list(labels['label'])[np.argmax(pred)]

    def get_model(self,model_path='ML/sector_classifier.h5') :
        embedding_dim = 64  # Embedding size for each token
        num_heads = 8  # Number of attention heads
        dff = 512  # Hidden layer size in feed forward network inside transformer
        max_len = self.MAX_LENGTH
        vocab_size = self.VOCAB_SIZE

        inputs = tf.keras.layers.Input(shape=(max_len,))
        embedding_layer = TokenAndPositionEmbedding(max_len, vocab_size, embedding_dim)
        x = embedding_layer(inputs)
        transformer_block = TransformerBlock(embedding_dim, num_heads, dff)
        x = transformer_block(x)
        x = tf.keras.layers.GlobalAveragePooling1D()(x)
        x = tf.keras.layers.Dropout(0.1)(x)
        x = tf.keras.layers.Dense(20, activation="relu")(x)
        x = tf.keras.layers.Dropout(0.1)(x)
        outputs = tf.keras.layers.Dense(26, activation="softmax")(x)

        model = tf.keras.Model(inputs=inputs, outputs=outputs)
        model.load_weights(model_path)
        return model

if __name__ == '__main__':
    model = AttentionModel()
    import time
    st = time.time()
    output = model.predict('react native 개발자 구인')
    print(output)
    fn = time.time() - st
    print(fn)