import re
from collections import Counter
import matplotlib.pyplot as pyplot
import pandas as pd
import seaborn as graph
import squarify
from string import punctuation
import spacy

original_text = open("text.txt").read() 
npl = spacy.load("ru_core_news_lg", disable=['parser', 'ner'])
spacy.lang.ru.stop_words.STOP_WORDS |= {" ", "  ", "   "}
spacy_stopwords = spacy.lang.ru.stop_words.STOP_WORDS

def spacy_tokenize(text):
    doc = npl.tokenizer(text)
    return [token.text for token in doc]

def spacy_lemmatize(text):
    doc = npl(text)
    return " ".join([token.lemma_ for token in doc if token.is_punct == False and token.is_stop == False]).split()

def remove_stopwords(tokens):
    cleaned_tokens = []
    for token in tokens:
        if token not in spacy_stopwords:
            cleaned_tokens.append(token)
    return cleaned_tokens

def clean_text(text):
    text = text.lower()
    text = re.sub(r'[^а-яА-Я $]', '', str(text))
    return text

def tokenize(text):
    text = text.lower()
    text = re.sub(r'[^а-яА-Я $]', '', str(text))
    return text.split()

def word_count(tokens):
    words = Counter()
    words.update(tokens)
    return words

def count(docs):
    words = Counter()
    appears_in = Counter()
    total_docs = len(docs)
    
    for doc in docs:
        words.update(doc)
        appears_in.update(set(doc))
    
    temp = list(zip(words.keys(), words.values()))
    
    wc = pd.DataFrame(temp, columns = ['word', 'count'])
    wc['rank'] = wc['count'].rank(method='first', ascending=False)
    total = wc['count'].sum()
    wc['pct_total'] = wc['count'].apply(lambda x: x / total)

    wc = wc.sort_values(by='rank')
    wc['cul_pct_total'] = wc['pct_total'].cumsum()

    temp2 = list(zip(appears_in.keys(), appears_in.values()))
    ac = pd.DataFrame(temp2, columns=['word', 'appears_in'])
    wc = ac.merge(wc, on='word')

    wc['appears_in_pct'] = wc['appears_in'].apply(lambda x: x / total_docs)
    return wc.sort_values(by='rank')


#tokens = tokenize(original_text)
#words = word_count(tokens)
#pandas_table = count([tokens])
#pandas_table_top20 = pandas_table[pandas_table['rank']<= 20]

text = clean_text(original_text)
spacy_lemmas = spacy_lemmatize(text)
spacy_tokens = spacy_tokenize(text)
cleaned_tokens = remove_stopwords(spacy_tokens)
cleaned_lemmas = remove_stopwords(spacy_lemmas)

words = word_count(cleaned_lemmas)
pandas_table = count([cleaned_lemmas])
pandas_table_top20 = pandas_table[pandas_table['rank']<= 20]
print(words.most_common(50))
   
#squarify.plot(sizes=pandas_table_top20['pct_total'], label=pandas_table_top20['word'], alpha=.8 )
#pyplot.axis('off')
graph.lineplot(x = 'rank', y = 'cul_pct_total', data = pandas_table)
pyplot.show()
