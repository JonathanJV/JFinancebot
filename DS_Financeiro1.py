#!/usr/bin/env python
# coding: utf-8

# In[2]:


from selenium import webdriver
from time import sleep
from os import path
import os
import pandas as pd
from tqdm.notebook import tqdm


# In[3]:


def coletar_dados_fundamentalista_status_invest():
    URL = 'https://statusinvest.com.br/acoes/busca-avancada'
    output = '/Users/jonathan_vieira/Downloads/statusinvest-busca-avancada.csv'
    
    if path.exists(output):
        print('... Removendo arquivo existente no diretório: {}'.format(output))
        os.remove(output)
        
    print('...Fazendo aquisição')
    driver = webdriver.Chrome(executable_path='/Users/jonathan_vieira/chromedriver')
    driver.get(URL)
    sleep(1)
    
    print('...Buscando elemento')
    button_buscar =driver.find_element_by_xpath(
        '//div/button[contains(@class,"find")]')
    
    button_buscar.click()
    sleep(2)
    
    
    """
        print('Fechando anúncio')
        button_fechar = driver.find_element_by_class_name('btn-close)
        button_fechar.click()
        sleep(1)
    """
    
    print('...Baixando dados para o disco...')
    button_download=driver.find_element_by_xpath(
    '//div/a[contains(@class, "btn-download")]')
        
    button_download.click()
    sleep(2)
    
    if path.exists(output):
        df=pd.read_csv(output, sep=';', decimal=',', thousands='.')
        print('...Fechando Driver')
        driver.close()
        return df
        
    else:
        print('Arquivo não encontrado')
        


# In[4]:


acoes= coletar_dados_fundamentalista_status_invest()


# In[5]:


acoes.columns = [c.strip() for c in acoes.columns]


# In[6]:


filter_fundamentalista = (acoes['ROE']> 10) &                          (acoes['MARG. LIQUIDA'] >10) &                          (acoes['MARGEM EBIT'] >15) &                          (acoes['DY'] >4) &                          (acoes['P/L'] <20) & (acoes['P/L'] > 0) &                         (acoes['DIVIDA LIQUIDA / EBIT'] < 2) &                          (acoes['PEG Ratio'] < 1.5) & (acoes['PEG Ratio'] >0)


# In[7]:


acoes[filter_fundamentalista]


# In[8]:


#usando Request para coletar dados de Fundos imobiliarios


# In[9]:


import requests


# In[10]:


def colentando_fundos_imobiliarios_funds_explorer():
    url='https://www.fundsexplorer.com.br/ranking'
    page = requests.get(url)
    df=pd.read_html(page.content, encoding='utf-8')[0]

    df= df.replace({'%': '', 'R\$':'', '\.':'',',':'.'}, regex=True)

    columns=df.columns
    
    for c in columns:
        if df[c].dtype==object:
            df[c]=df[c].str.strip()
            try:
                df[c]=df[c].astype(float)
            except Exception as e:
                print('Erro: Coluna:{} -- {}'.format(c,e))
                pass
    df['P/VPA'] =df['P/VPA']/100
    df.fillna(0, inplace=True)
    
    return df
        


# In[11]:


fiis=colentando_fundos_imobiliarios_funds_explorer()


# In[12]:


fiis


# In[13]:


def oportunidade_media_setor(df_, setor='Shoppings', label_setor='Setor'):
    media_setor=df_.groupby('Setor').agg(['mean', 'std'])
    df_setor=df_[df_[label_setor].isin([setor])]
    
    filter_=             (df_setor['QuantidadeAtivos'] > 5) &             (df_setor['Liquidez Diária'] >5000) &             (df_setor['P/VPA'] <1.0) &             (df_setor['VacânciaFísica'] <10) &             (df_setor['DY (12M)Acumulado']>media_setor.loc[setor, ('DY (12M)Acumulado', 'mean')])
    print('média do setor Yield: {}'.format(media_setor.loc[setor, ('DY (12M)Acumulado', 'mean')]))
    print('média do setor p/VPA: {}'.format(media_setor.loc[setor, ('P/VPA', 'mean')]))
    print('média do setor Ativos: {}'.format(media_setor.loc[setor, ('QuantidadeAtivos', 'mean')]))
    
    return df_setor[filter_]
    
    
    


# In[14]:


fiis['Setor'].unique()


# In[15]:


oportunidade_media_setor(fiis, setor='Shoppings')


# In[16]:


fiis.groupby('Setor').agg(['median', 'mean'])['DY (12M)Acumulado']


# In[17]:


#coletando dados de cotacao usando Yfinance


# In[18]:



# In[19]:


import yfinance as yf


# In[20]:


symbol= 'TAEE11.SA'#IBV=F
ticker=yf.Ticker(symbol)
ticker.history().head()


# In[21]:


#coletando dados de cotacao usando InvestPy


# In[22]:


# In[23]:


import investpy as ip


# In[24]:


symbol='TAEE11'
ip.get_stock_historical_data(stock=symbol,
                            from_date='01/07/2021',
                            to_date='01/08/2021',
                            country='brazil').head()


# In[25]:


#obtendo as maiores altas e maiores baixas


# In[26]:


overview= ip.get_stocks_overview(country='Brazil')


# In[27]:


overview['change_percentage']= pd.to_numeric(overview['change_percentage'].str.replace('%',''))


# In[28]:


overview.sort_values('change_percentage', ascending=True).iloc[:10]


# In[29]:


overview.sort_values('change_percentage', ascending=False).iloc[:10]


# In[30]:


#entendendo a analise técnica


# In[31]:


from pandas_datareader import data


# In[32]:


start_date='2020-01-01'
end_date='2021-08-01'

ibov=yf.Ticker('MGLU3.SA').history(start=start_date, end_date=end_date)


# In[33]:


#calculando rentabilidade


# In[34]:


ibov['pct_change']=ibov['Close'].pct_change()*100


# In[35]:


#calculando medias moveis


# In[36]:


ibov['mv_7d']=ibov['Close'].rolling(7).mean()
ibov['mv_21d']=ibov['Close'].rolling(21).mean()
ibov['mv_50d']=ibov['Close'].rolling(50).mean()
ibov['mv_200d']=ibov['Close'].rolling(200).mean()


# In[37]:


def bandas_de_bolling(data, period=20, std_factor=2):
    data['std']=data['Close'].rolling(period).std()
    data['mean']=data['Close'].rolling(period).mean()
    data['banda_superior']=data['mean']+data['std']* std_factor
    data['banda_inferior']=data['mean']-data['std']* std_factor


# In[38]:


bandas_de_bolling(ibov, period=20, std_factor=2)


# In[39]:


#visualizar dados


# In[40]:


import plotly.graph_objs as go


# In[41]:


def plotCandleStick(df_, name='ticket', showlegend=True, lines=[]):
    trace1 ={
        'x':df_.index,
        'open':df_.Open,
        'close':df_.Close,
        'high':df_.High,
        'low':df_.Low,
        'type':'candlestick',
        'name':name,
        'showlegend':showlegend
    }
    
    data= [trace1]
    layout=go.Layout()
    
    fig= go.Figure(data=data, layout=layout)
    
    if len(lines)>0:
        for c in lines:
            fig.add_trace(
            go.Scatter(x=list(df_.index),
                      y=df_[c],
                      mode='lines',
                      name=c))
    return fig


# In[42]:


plotCandleStick(ibov,
                showlegend=False,
                lines=['mv_7d','mv_21d','mv_50d','mv_200d','banda_superior','banda_inferior','mean'])


# In[43]:


import mplfinance as mpf


# In[44]:


filter_date= '2021-01-01'
bollinger = mpf.make_addplot(ibov.loc[filter_date:,['banda_superior', 'banda_inferior', 'mean']])
mpf.plot(ibov[filter_date:], addplot=bollinger, type='candle', mav=(7, 50), volume=True)


# In[45]:


mpf.plot(ibov['2021-05-01':'2021-08-01'], type='candle')


# In[46]:


mpf.plot(ibov['2021-05-01':'2021-08-01'], type='candle', mav=(7,21), volume=True)


# In[47]:


#Bot no Telegram


# In[48]:


class Bot():
    def __init__(self, token, chat_id=None):
        self.token=token
        self.chat_id=chat_id
        self.url='https://api.telegram.org/bot{}'.format(token)
    
    def send_message(self, msg,
                     disable_web_page_preview=False,
                     disable_notification=False,
                     parse_mode=['HTML', 'Markdown']):
        try:
            assert self.chat_id is not None, 'ERRO:chat_id is None, please set a chat_id'
            data= {'chat_id':self.chat_id, 'text':msg, 'disable_web_page_preview':disable_web_page_preview,
                  'disable_notification': disable_notification, 'parse_mode':parse_mode[0]}
            url=self.url + '/sendMessage'
            response=requests.post(url, data)
            return response
        except Exception as e:
            print('ERRO send_message:request Não Enviada: {}'.format(url))
            print(e)
            raise e
            
    def setChatId(self, chat_id):
        self.chat_id=chat_id


# In[49]:


token='1931963301:AAEKxzfDMcsFouIFyA49hjyJfYnucFgadF8'


# In[50]:


JFinancebot= Bot(token=token)
JFinancebot.setChatId("-511572903")


# In[51]:


JFinancebot.send_message(msg="oi")


# In[52]:


#Gerenciando Carteira


# In[53]:


def search_opportunities():
    acoes=coletar_dados_fundamentalista_status_invest()
    acoes.columns=[c.strip() for c in acoes.columns]
    filter_fundamentalista = (acoes['ROE']> 10) &                          (acoes['MARG. LIQUIDA'] >10) &                          (acoes['MARGEM EBIT'] >15) &                          (acoes['DY'] >4) &                          (acoes['P/L'] <20) & (acoes['P/L'] > 0) &                         (acoes['DIVIDA LIQUIDA / EBIT'] < 2) &                          (acoes['PEG Ratio'] < 1.5) & (acoes['PEG Ratio'] >0)
    tickers=acoes[filter_fundamentalista]['TICKER'].unique()
    cotacoes=[]
    
    for t in tqdm(tickers):
        ticker = yf.Ticker(t+'.SA')
        aux=ticker.history(period='10mo', interval='1h')
        if aux.shape[0] >0:
            aux['ticker']=t
            cotacoes.append(aux)
    df_cotacoes =pd.concat(cotacoes)
                           
    group_cotacoes =df_cotacoes.groupby('ticker')
    df_cotacoes_diario=group_cotacoes.resample('D').mean()
    df_cotacoes_diario.dropna(inplace=True)
                           
                           
    df_cotacoes_diario['ma_7d'] =df_cotacoes_diario['Close'].rolling(7).mean()
    df_cotacoes_diario['ma_21d'] =df_cotacoes_diario['Close'].rolling(21).mean()
    df_cotacoes_diario['ma_50d'] =df_cotacoes_diario['Close'].rolling(50).mean()
    df_cotacoes_diario['ma_200d'] =df_cotacoes_diario['Close'].rolling(200).mean()
                           
    period=20
    std_factor=2
    df_cotacoes_diario['std']=df_cotacoes_diario['Close'].rolling(period).std()
    df_cotacoes_diario['mean']=df_cotacoes_diario['Close'].rolling(period).mean()
    df_cotacoes_diario['banda_superior']=df_cotacoes_diario['mean']+df_cotacoes_diario['std']* std_factor
    df_cotacoes_diario['banda_inferior']=df_cotacoes_diario['mean']-df_cotacoes_diario['std']* std_factor
                           
    last_cotacao=df_cotacoes_diario.groupby('ticker').last()
                           
                           
    filter_oportunidades =(last_cotacao['Close']<last_cotacao['banda_inferior'])
    return last_cotacao[filter_oportunidades]
    


# In[54]:


opportunities=search_opportunities()


# In[55]:


opportunities.reset_index(inplace=True)


# In[56]:


opportunities


# In[57]:


link= 'https://statusinvest.com.br/acoes/'
msg_opportunities = ["<a href='{}{}'>#{}</a>".format(link, row['ticker'].lower(), row['ticker'])for i, row in opportunities.iterrows()]


# In[58]:


msg = '\n'.join(msg_opportunities)


# In[59]:


JFinancebot.send_message('{}'.format(msg))


# In[60]:


#agendar as tarefas


# In[61]:


import schedule


# In[62]:


def search_opportunities():
    print('...Buscando por Oportunidades')
    acoes=coletar_dados_fundamentalista_status_invest()
    acoes.columns=[c.strip() for c in acoes.columns]
    filter_fundamentalista = (acoes['ROE']> 10) &                          (acoes['MARG. LIQUIDA'] >10) &                          (acoes['MARGEM EBIT'] >15) &                          (acoes['DY'] >4) &                          (acoes['P/L'] <20) & (acoes['P/L'] > 0) &                         (acoes['DIVIDA LIQUIDA / EBIT'] < 2) &                          (acoes['PEG Ratio'] < 1.5) & (acoes['PEG Ratio'] >0)
    tickers=acoes[filter_fundamentalista]['TICKER'].unique()
    cotacoes=[]
    
    for t in tqdm(tickers):
        ticker = yf.Ticker(t+'.SA')
        aux=ticker.history(period='10mo', interval='1h')
        if aux.shape[0] >0:
            aux['ticker']=t
            cotacoes.append(aux)
    
    df_cotacoes =pd.concat(cotacoes)
    
    print('...Extraindo médias móveis')
    
    group_cotacoes =df_cotacoes.groupby('ticker')
    df_cotacoes_diario=group_cotacoes.resample('D').mean()
    df_cotacoes_diario.dropna(inplace=True)
                           
                           
    df_cotacoes_diario['ma_7d'] =df_cotacoes_diario['Close'].rolling(7).mean()
    df_cotacoes_diario['ma_21d'] =df_cotacoes_diario['Close'].rolling(21).mean()
    df_cotacoes_diario['ma_50d'] =df_cotacoes_diario['Close'].rolling(50).mean()
    df_cotacoes_diario['ma_200d'] =df_cotacoes_diario['Close'].rolling(200).mean()
    
    print('...Criando Pandas de Bollinger')
    period=20
    std_factor=2
    df_cotacoes_diario['std']=df_cotacoes_diario['Close'].rolling(period).std()
    df_cotacoes_diario['mean']=df_cotacoes_diario['Close'].rolling(period).mean()
    df_cotacoes_diario['banda_superior']=df_cotacoes_diario['mean']+df_cotacoes_diario['std']* std_factor
    df_cotacoes_diario['banda_inferior']=df_cotacoes_diario['mean']-df_cotacoes_diario['std']* std_factor
    
    
    print('...Filtrando dados baseado na analise técnica')
    last_cotacao=df_cotacoes_diario.groupby('ticker').last()
                           
                           
    filter_oportunidades =(last_cotacao['Close']<last_cotacao['banda_inferior'])
    last_cotacao[filter_oportunidades]
    last_cotacao.reset_index(inplace=True)
    

    print('... Enviando mensagem no telegram')
    link= 'https://statusinvest.com.br/acoes/'
    msg_opportunities = ["<a href='{}{}'>#{}</a>".format(link, row['ticker'].lower(), row['ticker'])for i, row in opportunities.iterrows()]
    msg = '\n'.join(msg_opportunities)
    JFinancebot.send_message('{}'.format(msg))


# In[63]:


import time


# In[64]:


time_to_msgs=['10:00', '11:00', '12:00','13:00', '14:00', '15:00', '16:00', '17:00', '18:00', '19:00', '12:45']

for h in time_to_msgs:
    schedule.every().monday.at(h).do(search_opportunities)
    schedule.every().tuesday.at(h).do(search_opportunities)
    schedule.every().wednesday.at(h).do(search_opportunities)
    schedule.every().thursday.at(h).do(search_opportunities)
    schedule.every().friday.at(h).do(search_opportunities)
    schedule.every().saturday.at(h).do(search_opportunities)
    schedule.every().sunday.at(h).do(search_opportunities)
    
while True:
    schedule.run_pending()
    time.sleep(10)


# In[ ]:




