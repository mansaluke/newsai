#import tensorflow as tf
#import tensorflow_hub as hub
from dfconvert import df_store
#hub_url = "https://tfhub.dev/google/tf2-preview/nnlm-en-dim128/1"
#embed = hub.KerasLayer(hub_url)
#embeddings = embed(["A long sentence.", "single-word", "http://example.com"])
#print(embeddings.shape, embeddings.dtype)


if __name__ == "__main__":
    df = df_store('all_stories.h5').load_df()
    print(df)
    print(df.shape)