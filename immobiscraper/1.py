from immobiscraper import *
url = "https://www.immobiliare.it/affitto-case/padova/"
case = Immobiliare(url)
case.find_all_houses()
df = case.df_
df.head()