# https://jupyterhub.datasmart.apps.generali.fr/user/b010edv/proxy/8501/


import streamlit as st
import pandas as pd
import numpy as np
import pickle as pkl
import plotly.express as px
from plotly.subplots import make_subplots
import plotly.graph_objects as go
import matplotlib.pyplot as plt
import plotly.figure_factory as ff

st.set_page_config(layout="wide",
                   page_title='Résiliations infra-annuelles en santé',
                   page_icon='245367.png'
                  )




###########################
###     Fonctions
###########################

def mise_en_forme(df):
    
    df['values_list'] = df[df.columns[2:].tolist()].values.tolist()

    name_list = ['nbCli_top_santeis_senior',
           'nbCtr_top_santeis_senior', 'nbCli_top_santeis_clas',
           'nbCtr_top_santeis_clas', 'nbCli_top_ideo', 'nbCtr_top_ideo',
           'nbCli_top_tns', 'nbCtr_top_tns', 'nbCli_top_part', 'nbCtr_top_part',
           'nbCli_top_autre', 'nbCtr_top_autre'] * df.shape[0]
    print(df)

    df = df[['mois', 'libreseaucalcule', 'values_list']].explode('values_list')
    
    print(name_list)

    df['name_list'] = name_list

    df['type'] = df['name_list'].apply(lambda x: x.split('_')[0])
    df['produit'] = df['name_list'].apply(lambda x: x.split('_')[2:])
    df['produit'] = df['produit'].apply(lambda x: '_'.join(x))

    df['annee'] = df['mois'].apply(lambda x: str(x)[:4])
    df['mois_str'] = df['mois'].apply(lambda x: int(str(x)[4:]))

    df = df[df.values_list.notnull()]

    df['mois_str'] = pd.to_datetime(df['mois_str'], format='%m').dt.month_name().str.slice(stop=3)
    
    return df





#########################################################################################
###   Chargement et retraitement des dernières données (statiques pour l'instant)
#########################################################################################

stock_df = pkl.load( open( '../output/2022_07_06-14_36_38_stock_df', "rb" ) )
resil_df = pkl.load( open( '../output/2022_07_06-14_36_38_resil_mois_df', "rb" ) )

stock_df = mise_en_forme(stock_df)
stock_df = stock_df.rename(columns={'values_list': 'stock'})

resil_df = mise_en_forme(resil_df)
resil_df = resil_df.rename(columns={'values_list': 'resil'})

df = stock_df.merge(resil_df[['mois', 'libreseaucalcule', 'name_list', 'resil']], on=['mois', 'libreseaucalcule', 'name_list'])
df['tx_resil'] = df.resil/df.stock

print(df)

aaaa = int(max(df.annee.unique()))
aaaa_moins_1 = str(aaaa-1)
aaaa = str(aaaa)
aaaa_last = max(df.mois.unique())
aaaa_last_moins_1 = str(int(aaaa_last)-100)
aaaa01 = aaaa+'01'
aaaa01_moins_1 = aaaa_moins_1+'01'



reseaux = ['AGENT', 'COURTIER', 'SALARIE GPROX']
resil_deb = []

for i in reseaux:
    produits = sorted(df[(df.type=='nbCtr') & (df.libreseaucalcule == i)].produit.unique())
    for j in produits:
        tmp = df[(df.produit==j) & (df.type=='nbCtr') & (df.libreseaucalcule == i) & (df.annee==aaaa_moins_1) & (df.mois <= aaaa_last_moins_1)].resil.sum()/df[(df.annee == aaaa_moins_1) & (df.produit==j) & (df.type=='nbCtr') & (df.libreseaucalcule == i)].resil.sum()
        resil_deb.append(tmp)
tx = df[(df.type=='nbCtr')].groupby(['annee', 'libreseaucalcule', 'produit'], as_index=False).agg({'stock':'first', 'resil':'sum'})
tx['taux_annuel'] = np.round(tx.resil/tx.stock*100, 1)

tx2 = pd.pivot_table(data=tx, index=['libreseaucalcule', 'produit'], columns=['annee'], values=['stock'])
tx2.columns = ['2019', aaaa_moins_1, aaaa]
for col in tx2:
    tx2[col] = tx2[col].apply(lambda x: str(np.round(x/1000, 0))[:-2]+'k')

tx = pd.pivot_table(data=tx, index=['libreseaucalcule', 'produit'], columns=['annee'], values=['taux_annuel'])
tx.columns = ['2019', aaaa_moins_1, aaaa]
tx[aaaa] = np.round(tx[aaaa]/resil_deb, 1)





#####################################
###     Boutons de sélection
#####################################

st.sidebar.image('245367.png', use_column_width=True)

produits = {'Agents':['Ideo', 'La Santé TNS', 'La Santé Part'], 'Courtiers':['Ideo', 'La Santé TNS', 'La Santé Part'], 'Réseau Salarié':['Santeis Senior', 'Santeis Classique']}

select_reseau = st.sidebar.selectbox('Réseau', ['Agents', 'Courtiers', 'Réseau Salarié'], key='1')
select_prd = st.sidebar.selectbox('Produit', produits[select_reseau], key='1')
select_nb = st.sidebar.selectbox('Affichage en nombre de...', ['Clients', 'Contrats'], key='1')

reseau = {'Agents':'AGENT', 'Courtiers':'COURTIER', 'Réseau Salarié':'SALARIE GPROX'}
prd = {'Ideo':'ideo', 'TNS':'tns', 'Particulier':'part', 'Autres':'autre', 'Santeis Senior':'santeis_senior', 'Santeis Classique':'santeis_clas'}
nb = {'Clients':'nbCli', 'Contrats':'nbCtr'}





#####################################
###     Données selectionnées
#####################################

df_graph = df[(df.libreseaucalcule == reseau[select_reseau]) & (df.type == nb[select_nb]) & (df.produit == prd[select_prd])]
df_graph.index = df_graph.mois

a = df_graph[(df_graph.annee==aaaa) & (df_graph.mois <= aaaa_last)].resil.sum()
b = df_graph[df_graph.annee==aaaa_moins_1].resil.sum()
c = df_graph[(df_graph.annee==aaaa_moins_1) & (df_graph.mois <= aaaa_last_moins_1)].resil.sum()
d = df_graph[df_graph.annee=='2019'].resil.sum()
stock_2019 = df_graph['stock']['201901']
stock_n_moins_1 = df_graph['stock'][aaaa01_moins_1]
stock_n = df_graph['stock'][aaaa01]

tx_n_moins_2 = np.round(d/stock_2019*100, 1)
tx_n_moins_1 = np.round(b/stock_n_moins_1*100, 1)
tx_n = np.round((a*b/c)/stock_n*100, 1)

df_taux = pd.pivot_table(df_graph, index='annee', columns='mois_str', values='tx_resil')
df_taux.index.name = None
df_taux.columns.name = None
df_taux = df_taux[['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']]
df_taux.columns = ['Jan', 'Fev', 'Mar', 'Avr', 'Mai', 'Juin', 'Juil', 'Aout', 'Sept', 'Oct', 'Nov', 'Dec']

df_resil = pd.pivot_table(df_graph, index='annee', columns='mois_str', values='resil')
df_resil.index.name = None
df_resil.columns.name = None
df_resil = df_resil[['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']]
df_resil.columns = ['Jan', 'Fev', 'Mar', 'Avr', 'Mai', 'Juin', 'Juil', 'Aout', 'Sept', 'Oct', 'Nov', 'Dec']
df_resil.reset_index(inplace=True)
df_resil['index'] = ['2019 (stock = '+str(int(stock_2019))+')', aaaa_moins_1+' (stock = '+str(int(stock_n_moins_1))+')', aaaa+' (stock = '+str(int(stock_n))+')']
df_resil.rename(columns={'index':''}, inplace=True)
df_resil = df_resil.iloc[1: , :]

df_stock = pd.pivot_table(df_graph, index='annee', columns='mois_str', values='stock')
df_stock.index.name = None
df_stock.columns.name = None
df_stock = df_stock[['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']]
df_stock.reset_index(inplace=True)
df_stock.rename(columns={'index':''}, inplace=True)



    
    
#####################################
###     Mise en forme de la page
#####################################

st.title("Suivi des résiliations infra-annuelles en santé")

#----------------
#-- Recap
#----------------

with st.expander("Récapitulatif des taux annuels"):
    
    
    def indic(col, reseau, produit):
        
        st_nm1 = tx2.loc[reseau, produit][aaaa_moins_1]
        st_n = tx2.loc[reseau, produit][aaaa]
        
        fig_one = go.Figure()

        fig_one.add_trace(go.Indicator(
            mode = "number",
            value = tx.loc[reseau, produit][aaaa_moins_1],
            number = {"font": {"color":'#FB8F28', "size":35},"suffix":'%'},
            title = {"text": "<span style='font-weight:bold;font-style:italic;font-size:2em;color:#FB8F28'>"+aaaa_moins_1+"</span><br><span style='font-size:1.7em;font-style:italic;color:#FB8F28'>stock : "+st_nm1+"</span><br>"}))

        fig_one.update_layout(
            autosize=False,
            width=130,
            height=110,
            margin=dict(l=0,r=0,b=0,t=0)
        )


        fig_two = go.Figure()

        fig_two.add_trace(go.Indicator(
            mode = "number",
            value = tx.loc[reseau, produit][aaaa],
            number = {"font": {"color":'#E1220F', "size":35},"suffix":'%'},
            title = {"text": "<span style='font-weight:bold;font-style:italic;font-size:2em;color:#E1220F'>"+aaaa+"</span><br><span style='font-size:1.7em;font-style:italic;color:#E1220F'>stock : "+st_n+"</span><br>"}))

        fig_two.update_layout(
            autosize=False,
            width=130,
            height=110,
            margin=dict(l=0,r=0,b=0,t=0)
        )
        
        nom_produit = {'ideo':'Idéo', 'tns':'La Santé TNS', 'part':'La Santé Part', 'santeis_clas':'Santéis Classique', 'santeis_senior':'Santéis Senior'}
        text = "<p style='text-align: center;font-size:100%;padding: 0'>"+nom_produit[produit]+"<p><hr style='height: 1px; width: 99%; background-color: #848484'/>"

        with col:
            st.markdown(text, unsafe_allow_html=True)
            st.plotly_chart(fig_one)
            st.plotly_chart(fig_two)
    
    
    c5, c6, c7 = st.columns((3, 3, 2))
    
    c5.markdown("<h3 style='text-align: center;font-size:150%;padding: 0'>Agents</h3><hr style='height: 1px; width: 99%; background-color: #848484'/>", unsafe_allow_html=True)
    c6.markdown("<h3 style='text-align: center;font-size:150%;padding: 0'>Courtiers</h3><hr style='height: 1px; width: 99%; background-color: #848484'/>", unsafe_allow_html=True)
    c7.markdown("<h3 style='text-align: center;font-size:150%;padding: 0'>Réseau salarié</h3><hr style='height: 1px; width: 99%; background-color: #848484'/>", unsafe_allow_html=True)


    c11, c12, c13, c14, c15, c16, c17, c18 = st.columns((1, 1, 1, 1, 1, 1, 1, 1))
    indic(c11, 'AGENT', 'ideo')
    indic(c12, 'AGENT', 'tns')
    indic(c13, 'AGENT', 'part')
    indic(c14, 'COURTIER', 'ideo')
    indic(c15, 'COURTIER', 'tns')
    indic(c16, 'COURTIER', 'part')
    indic(c17, 'SALARIE GPROX', 'santeis_clas')
    indic(c18, 'SALARIE GPROX', 'santeis_senior')
    
    
    
    

c1, c2 = st.columns((5, 1))

#----------------
#-- Graphique
#----------------

layout = go.Layout(
    title="Taux de résiliations mensuels - "+select_reseau+"/"+select_prd+" (en "+select_nb+")",
    plot_bgcolor="#FFFFFF",
    barmode="stack",
    xaxis={'linecolor':"#BCCCDC"},
    yaxis={'linecolor':"#BCCCDC", 'tickformat':".1%"},
    autosize=False,
    width=950,
    height=450,
    font={'size':15}
)

fig = go.Figure(
    data = go.Scatter(
        x=df_graph[df_graph.annee==aaaa_moins_1].mois_str,
        y=df_graph[df_graph.annee==aaaa_moins_1].tx_resil,
        mode='lines+markers', marker_color="#FB8F28", name=aaaa_moins_1),
    layout=layout)

fig.add_trace(
    go.Scatter(
        x=df_graph[df_graph.annee==aaaa].mois_str,
        y=df_graph[df_graph.annee==aaaa].tx_resil,
        mode='lines+markers', marker_color="#E1220F", name=aaaa))

fig.update_yaxes(showline=True, gridcolor='#E7E7E7', rangemode="tozero")

c1.plotly_chart(fig)



#----------------
#-- Indicateurs
#----------------

fig2 = go.Figure()

fig2.add_trace(go.Indicator(
    mode = "number",
    value = tx_n_moins_1,
    number = {"font": {"color":'#FB8F28', "size":60},"suffix":'%'},
    title = {"text": "<span style='font-weight:bold;font-style:italic;font-size:1.8em;color:#FB8F28'>"+aaaa_moins_1+"</span><br><span style='font-size:1em;font-style:italic;color:#FB8F28'>Taux annuel</span><br>"}))

fig2.update_layout(
    autosize=False,
    width=150,
    height=180,
    margin=dict(l=0,r=0,b=0,t=0)
)


fig3 = go.Figure()

fig3.add_trace(go.Indicator(
    mode = "number",
    value = tx_n,
    number = {"font": {"color":'#E1220F', "size":60},"suffix":'%'},
    title = {"text": "<span style='font-weight:bold;font-style:italic;font-size:1.8em;color:#E1220F'>"+aaaa+"</span><br><span style='font-size:1em;font-style:italic;color:#E1220F'>Taux annuel</span><br>"}))

fig3.update_layout(
    autosize=False,
    width=150,
    height=180,
    margin=dict(l=0,r=0,b=0,t=0)
)

with c2:
    st.markdown('<br><br><br>', unsafe_allow_html=True)
    st.plotly_chart(fig2)
    st.plotly_chart(fig3)


    
#----------------------
#-- Table de données
#----------------------    

# c3, c4 = st.scolumns((7, 3))


fig4 = go.Figure(data=[go.Table(
    columnorder=list(range(1, 14)),
    columnwidth = [70] + [23],
    header=dict(values=list(df_resil.columns),
                fill_color=['#FFFFFF']+['#F5F4F3'],
                align=['center'], font_size=16, height=30),
    cells=dict(values=df_resil.transpose().values.tolist(),
               fill_color=['#F5F4F3']+['#FFFFFF'],
               align=['left']+['center'], font_size=16, height=30)
            )
])

fig4.update_layout(
    autosize=False,
    width=800,
    height=200,
    margin=dict(l=0,r=0,b=0,t=0)
)

st.markdown('**Nombre de résiliations**')
st.plotly_chart(fig4)

