import re
from collections import Counter
import matplotlib.pyplot as pyplot
import pandas as pd
import seaborn as graph
import squarify
from string import punctuation
import spacy
import nltk
nltk.download('punkt')
nltk.download('punkt_tab')


original_text = open("text.txt").read() 
nlp = spacy.load("ru_core_news_lg", disable=['parser', 'ner'])
spacy.lang.ru.stop_words.STOP_WORDS |= {" ", "  ", "   ", "    ","     ", "      ","       ", "        " ,
"         ", "          ", "                   ", "ни", "ст", "прим", "см"}
spacy_stopwords = spacy.lang.ru.stop_words.STOP_WORDS
nlp.max_length = 2000000

def get_average_sentence_length(text):
    senteces = nltk.sent_tokenize(text, language = "russian")
    words_in_sentences = [len(tokenize(sentence)) for sentence in senteces]
    average_sentence_length = sum(words_in_sentences)/len(senteces)
    return average_sentence_length

def spacy_tokenize_simple(doc):
    return [token.text for token in doc]

def spacy_tokenize_complex(doc, spacy_stopwords):
    keywords = []
    for token in doc:
        if(token.text in spacy_stopwords):
            continue
        else:
            keywords.append(token.text)
    return keywords

def spacy_lemmatize(doc):
    return " ".join([token.lemma_ for token in doc if token.is_punct == False and token.is_stop == False]).split()

def remove_stopwords(tokens, spacy_stopwords):
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

text = clean_text(original_text)
doc = nlp(text)
spacy_tokens_before = spacy_tokenize_simple(doc)
print("Tokens with stop words: ", len(spacy_tokens_before))
print("Types: ", len(set(spacy_tokens_before)))
spacy_tokens_before = Counter([token.pos_ for token in doc])
print(spacy_tokens_before)
spacy_lemmas = spacy_lemmatize(doc)
print("Lemmas:", len(set(spacy_lemmas)))
print("Average sentence length: ", get_average_sentence_length(original_text))
spacy_tokens_after = spacy_tokenize_complex(doc, spacy_stopwords)
print("Tokens without stop words: ", len(spacy_tokens_after))
spacy_tokens_after = remove_stopwords(spacy_tokens_after, spacy_stopwords)
spacy_tokens_after = Counter([token.pos_ for token in doc if token.text not in spacy_stopwords])
cleaned_lemmas = remove_stopwords(spacy_lemmas, spacy_stopwords)
words = word_count(cleaned_lemmas)
pandas_table = count([cleaned_lemmas])
pandas_table_top20 = pandas_table[pandas_table['rank']<= 20]

#pyplot.axis('off')
figure, axis = pyplot.subplots(2, 2)
#squarify.barplot(sizes=pandas_table_top20['pct_total'], label=pandas_table_top20['word'], alpha=.8 )
graph.lineplot(x = 'rank', y = 'count', data = pandas_table, ax = axis[0][0])
graph.barplot(x = 'count', y = 'word', data = pandas_table_top20, ax = axis[0][1])
graph.barplot(x = spacy_tokens_before.keys(), y = spacy_tokens_before.values(), ax = axis[1][0])
graph.barplot(x = spacy_tokens_after.keys(), y = spacy_tokens_after.values(), ax = axis[1][1])
graph.barplot
pyplot.show()
