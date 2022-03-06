##############################################################################
# Créez et utilisez une base de données immobilière avec Python
##############################################################################
"""Author :
    Abdelmajid EL HOU - Data Analyst
    Version 1.1 """

# Objectif de ce petit projet est de créer et utiliser une base de données SQL
# à l'aide du librairie

# 01. Les librairies
import sqlite3 as sql
import pandas as pd
#import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt

# 02. fonction pour verifier les valeurs manquantes par colonne
def check_na(dataframe):
    """
    Parameters
    ----------
    df : TYPE dataframe
        inter the dataframe name.

    Returns
    -------
    Print number of missing values per column.

    """
    print(dataframe.isna().sum())

# 03. Importer et nettoyer les données
local   = pd.read_csv("data/local.csv", sep=';')
bien    = pd.read_csv("data/bien.csv", sep=';')
commune = pd.read_csv("data/commune.csv", sep=';')


# 04. Merger les fichiers
## merger les fichiers dans un seul tableau
df_bien_local = pd.merge(bien, local, how='left', on="local_id")
df_all = pd.merge(df_bien_local, commune, how='left', on='commune_id')

# creer une base de données SQL pour stocker les données
con = sql.connect('my.db')

# sauvegarder le tableau dans SQlite
df_all.to_sql(name='df_immo', con=con, if_exists='replace')

# Partie 2 - Analyse exploratoire des données
## Lire la base de données SQLite
con = sql.connect("my.db")
## Lire le tableau
df2 = pd.read_sql_query("SELECT * from df_immo", con)
## fermer la connection
con.close()


# Convertir la colonne date en format date
df2['date_mutation'] = pd.to_datetime(df2['date_mutation'])

# verifier les valeurs manquantes par colonne
check_na(df2)

# afficher la distribution des variables sur mon jeu de données
sns.pairplot(df2.iloc[:,2:], hue='type_local')
# exporter la figure
plt.savefig('pairplot.png')

# Representation graphique de l'evolution des prix de ventes des biens
plt.figure(figsize=(20, 5))
df = df2.set_index(['date_mutation'])
df.loc["2020"]['valeur_fonciere'].resample('3M').sum().\
                plot(label="Valeur fonciere par trimestre", \
                     lw=2, ls='-', alpha=0.8)
df.loc["2020"]['valeur_fonciere'].resample('M').sum().\
                plot(label="Valeur fonciere par mois", \
                     lw=2, ls='-', alpha=0.8)
df.loc["2020"]['valeur_fonciere'].resample('w').sum().\
                plot(label="Valeur fonciere par semaine",\
                     lw=2, ls='-', alpha=0.8)
plt.xlabel("Temps")
plt.ylabel("Valeur fonciere (euro)")
plt.legend(fontsize=15, loc = "upper left")
plt.grid(b=None)
plt.savefig('evol_valFonc_trimest_mois_annee.png', dpi = 199) # save plots
plt.show()

# Representation graphique de l'evolution des prix de ventes des maisons 
# et appartements
dt_appart = df[(df["type_local"] == "Appartement")]
dt_maison = df[(df["type_local"] == "Maison")]

plt.figure(figsize=(20, 5))
dt_appart.loc["2020"]['valeur_fonciere'].resample('M').sum().\
    plot(label="Valeur fonciere par mois - Maisons", lw=2, ls='-', alpha=0.8)
dt_maison.loc["2020"]['valeur_fonciere'].resample('M').sum().\
    plot(label="Valeur fonciere par mois - Appartements", lw=2, \
         ls='-', alpha=0.8)

plt.xlabel("Temps")

plt.ylabel("Valeur fonciere (euro)")
plt.legend(fontsize=15, loc = "upper left")
plt.grid(b=None)
plt.savefig('evol_valFonc_mois_maison_appart.png', dpi = 199) # save plots
plt.show()


# Nombre total d’appartements vendus au 1er semestre 2020.
nb_appart_s1_2020 = df2[(df2.date_mutation > '2020-01-01') & \
                        (df2.date_mutation < '2020-07-01') & \
                            (df2["type_local"] == "Appartement")]

# Proportion des ventes d’appartements par le nombre de pièces.
prop_ventes_app_nbPieces = df2[(df2.type_local == "Appartement") & \
                               (df2.nature_mutation == "Vente")].\
        groupby('nombre_pieces_principales').\
            size().reset_index(name='Total')

# presentation graphique
plt.figure(figsize=(10, 10))
sns.barplot(x = 'nombre_pieces_principales', y = 'Total', \
            data = prop_ventes_app_nbPieces)
plt.savefig('prop_ventes_app_par_nb_pieces.png', dpi = 199)
plt.show()

# Liste des 10 départements où le prix du mètre carré est le plus élevé.
## calculer le prix du m2
df2['prix_m2'] = df2["valeur_fonciere"]/df2["surface_carrez"]

# afficher les dep avec le prix du m2 le plus elevé
prix_dep = df2[["code_departement", "prix_m2"]].sort_values(by=['prix_m2'], \
                                                            ascending=False)

prix_dep.drop_duplicates(subset=['code_departement']).head(10)

# Prix moyen du mètre carré d’une maison en Île-de-France.
df2[(df2.type_local == 'Maison') & (df2["code_departement"].\
    isin(["75","77", "78", "91", "92", "93", "94", "95"]))][["prix_m2"]].mean()

# Liste des 10 appartements les plus chers avec le département et le nombre 
# de mètres carrés.
df2[(df2.type_local == 'Appartement')][["bien_id", "code_departement", \
            "valeur_fonciere"]].sort_values(by='valeur_fonciere', \
                                            ascending=False).head(10)

# Taux d’évolution du nombre de ventes entre le premier et le second trimestre de 2020.
R1 = df2[(df2.date_mutation >= '2020-01-01') & \
         (df2.date_mutation <= '2020-03-30') & \
             (df2.nature_mutation == 'Vente')].shape[0]
R2 = df2[(df2.date_mutation >= '2020-04-01') & \
         (df2.date_mutation <= '2020-06-30') & \
             (df2.nature_mutation == 'Vente')].shape[0]
print(round((R2 - R1) / R1 * 100, 2), "%")

# Différence en pourcentage du prix au mètre carré entre un appartement de 2 
# pièces et un appartement de 3 pièces.
app_2p = df2[(df2.type_local == 'Appartement') & \
             (df2.nombre_pieces_principales == 2)][['prix_m2']].mean()
app_3p = df2[(df2.type_local == 'Appartement') & \
             (df2.nombre_pieces_principales == 3)][['prix_m2']].mean()

print(round(100 - (app_3p[0] * 100) /  app_2p[0],2), "%")

# les moyennes de valeurs foncières pour le top 20 des communes.
con = sql.connect("my.db")
df3 = pd.read_sql_query("SELECT commune, round(avg(valeur_fonciere),2) as Moyenne FROM df_immo WHERE commune_id NOTNULL GROUP BY commune ORDER BY Moyenne DESC LIMIT 20", con)
#fermer la connection
con.close()

