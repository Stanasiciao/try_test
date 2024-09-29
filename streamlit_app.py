#Les installations pour le deployement sur le cloud 

import streamlit as st
import pandas as pd
import numpy as np
from functools import reduce
from pyxlsb import open_workbook
from pyxlsb import workbook


# Titre de l'application
st.title("Analyse des données 2G, 3G et 4G")

# Fonction pour lire et traiter les données avec mise en cache
@st.cache_data
def process_data(uploaded_file):
    sheets_dict = pd.read_excel(uploaded_file, sheet_name=None, engine='pyxlsb', header=0, parse_dates=True)
    return sheets_dict

# Téléchargement des fichiers
uploaded_file_2G = st.file_uploader("Téléchargez votre fichier Excel 2G", type=["xlsb"], key="2G")
uploaded_file_3G = st.file_uploader("Téléchargez votre fichier Excel 3G", type=["xlsb"], key="3G")
uploaded_file_4G = st.file_uploader("Téléchargez votre fichier Excel 4G", type=["xlsb"], key="4G")

# Dictionnaire pour stocker les DataFrames
dataframes = {}

# Analyse des fichiers téléchargés
if st.button("Lancer l'analyse"):
    if uploaded_file_2G is not None:
        dataframes['2G'] = process_data(uploaded_file_2G)
        st.success("Fichier 2G analysé.")
    if uploaded_file_3G is not None:
        dataframes['3G'] = process_data(uploaded_file_3G)
        st.success("Fichier 3G analysé.")
    if uploaded_file_4G is not None:
        dataframes['4G'] = process_data(uploaded_file_4G)
        st.success("Fichier 4G analysé.")

    # Traitement des données pour chaque technologie
    for tech, sheets in dataframes.items():
        if tech == '2G':
            # Accéder aux DataFrames pour la technologie 2 G
            df_availability_2G = sheets['2G_CELL_DISPONIBILITY']
            df_trafic_2G = sheets['2G_TRAFFIC_ERLANG']

            # Remplacer les en-têtes problématiques par des noms valides
            for df in [df_availability_2G, df_trafic_2G]:
                df.columns = df.columns.str.replace(r'#NAME\?', 'Invalid_Name', regex=True)
                df.columns = df.columns.str.replace(r'#N/A', 'Invalid_Name', regex=True)

            # Renommer les colonnes de dates
            date_columns = df_availability_2G.columns[2:16]
            df_availability_2G.columns.values[2:16] = [f'Date_{i}' for i in range(1, len(date_columns) + 1)]
            df_trafic_2G.columns.values[2:16] = [f'Date_{i}' for i in range(1, len(date_columns) + 1)]

            # Supprimer les colonnes non utiles
            columns_to_drop = [f'A.{i}' for i in range(25)]
            df_availability_2G.drop(columns=columns_to_drop, errors='ignore', inplace=True)
            df_trafic_2G.drop(columns=columns_to_drop, errors='ignore', inplace=True)

            # Filtrer les données
            columns_to_check = [f'Date_{i}' for i in range(1, 15)]
            df_availability_zero_2G = df_availability_2G[(df_availability_2G[columns_to_check] == 0).any(axis=1)]
            df_2G_trafic_zero = df_trafic_2G[(df_trafic_2G[columns_to_check] == 0).any(axis=1)]

            # Affichage des données filtrées pour la technologie 2 G
            st.subheader("Données filtrées 2G ")
            st.write("Disponibilité degradée 2G:")
            st.dataframe(df_availability_zero_2G)
            st.write("Trafic degradée 2G:")
            st.dataframe(df_2G_trafic_zero)

            # Les sites où il y a indisponibilité sur les trois derniers jours
            required_columns = ['Unnamed: 0', 'Unnamed: 1', 'Date_12', 'Date_13', 'Date_14', 'site', 'region', 'action', 'cause']
            
            missing_columns = [col for col in required_columns if col not in df_availability_zero_2G.columns]
            
            if missing_columns:
                st.warning(f"Les colonnes suivantes sont manquantes dans la disponibilité : {missing_columns}")
            else:
                df_availability_3jours = df_availability_zero_2G[required_columns]
                st.subheader("Données filtres 2G sur les 3 derniers jours")
                st.write("Disponibilité 2G degradée sur les 3 derniers jours:")
                st.dataframe(df_availability_3jours)

                if not df_2G_trafic_zero.empty:
                    df_2G_trafic_3jours = df_2G_trafic_zero[required_columns]
                    st.write("Données 2G degradée sur les 3 derniers jours :")
                    st.dataframe(df_2G_trafic_3jours)

        elif tech == '3G':
            # Accéder aux DataFrames pour la technologie 3 G
            df_availability_3G = sheets['availability']
            df_voice_3G = sheets['voice']
            df_data_3G = sheets['trafficgb']
            df_speech_drop_3G = sheets['speech drop']

            # Remplacer les en-têtes problématiques par des noms valides
            for df in [df_availability_3G, df_voice_3G, df_data_3G, df_speech_drop_3G]:
                df.columns = df.columns.str.replace(r'#NAME\?', 'Invalid_Name', regex=True)
                df.columns = df.columns.str.replace(r'#N/A', 'Invalid_Name', regex=True)

            # Remplacer 'div' par NaN dans la dataframe df_3G_speech_drop
            df_speech_drop_3G.replace('#DIV/0', 0.0, inplace=True)
            df_speech_drop_3G.fillna(0.0, inplace=True)


            # Renommer les colonnes de dates
            date_columns = df_availability_3G.columns[2:16]
            for df in [df_availability_3G, df_voice_3G, df_data_3G, df_speech_drop_3G]:
                df.columns.values[2:16] = [f'Date_{i}' for i in range(1, len(date_columns) + 1)]

            # Supprimer les colonnes non utiles
            columns_to_drop = [f'A.{i}' for i in range(25)]
            for df in [df_availability_3G, df_voice_3G, df_data_3G, df_speech_drop_3G]:
                df.drop(columns=columns_to_drop, errors='ignore', inplace=True)

            # Filtrer les données
            columns_to_check = [f'Date_{i}' for i in range(1, 15)]
            df_availability_zero_3G = df_availability_3G[(df_availability_3G[columns_to_check] == 0).any(axis=1)]
            df_data_zero_3G = df_data_3G[(df_data_3G[columns_to_check] == 0).any(axis=1)]
            df_voice_zero_3G = df_voice_3G[(df_voice_3G[columns_to_check] == 0).any(axis=1)]
            df_speech_drop_zero_3G=df_speech_drop_3G[df_speech_drop_3G[columns_to_check].gt(0.02).any(axis=1)]


            # Affichage des données filtrées pour la technologie 3 G
            st.subheader("Données filtrées 3G")
            st.write("Disponibilité degradée 3G:")
            st.dataframe(df_availability_zero_3G)
            st.write("Donnees degradées 3G:")
            st.dataframe(df_data_zero_3G)
            st.write("Voix degradée 3G:")
            st.dataframe(df_voice_zero_3G)
            st.write("Speech Drop degradée 3G:")
            st.dataframe(df_speech_drop_zero_3G)
            
            required_columns = ['Unnamed: 0', 'Unnamed: 1', 'Date_12', 'Date_13', 'Date_14', 'site', 'region', 'action', 'cause']
            
            missing_columns = [col for col in required_columns if col not in df_availability_zero_3G.columns]
            
            if missing_columns:
                st.warning(f"Les colonnes suivantes sont manquantes dans la disponibilité : {missing_columns}")
            else:
                # Les sites où il y a indisponibilité sur les trois derniers jours
                df_availability_three_days_df=df_availability_zero_3G[['Unnamed: 0', 'Unnamed: 1',"Date_12","Date_13","Date_14","site","region","action","cause"]]
                df_3G_data_3_jours = df_data_zero_3G[['Unnamed: 0', 'Unnamed: 1','Date_12', 'Date_13', 'Date_14', 'site', 'region', 'action', 'cause']]
                df_3G_Voice_3jours = df_voice_zero_3G[['Unnamed: 0', 'Unnamed: 1','Date_12', 'Date_13', 'Date_14', 'site', 'region', 'action', 'cause']]
                df_3G_speech_drop_3_jours = df_speech_drop_zero_3G[['Unnamed: 0', 'Unnamed: 1','Date_12', 'Date_13', 'Date_14', 'site', 'region', 'action', 'cause']]


                st.subheader("Données des 3 derniers jours pour la technologie 3G")
                st.write("Disponibilité 3G degradée sur les 3 derniers jours:")
                st.dataframe(df_availability_three_days_df)
                st.write("Données 3G degradées sur les 3 derniers jours:")
                st.dataframe(df_3G_data_3_jours)
                st.write("Voix 3G degradée sur les 3 derniers jours:")
                st.dataframe(df_3G_Voice_3jours)
                st.write("Speech Drop 3G degradéesur les 3 derniers jours:")
                st.dataframe(df_3G_speech_drop_3_jours)


        elif tech == '4G':
             # Accéder aux DataFrames pour la technologie 4 G
             if "availability_auto" in sheets:
                 # Lire avec skiprows pour ignorer les trois premières lignes si nécessaire
                 df_availability_auto = pd.read_excel(uploaded_file_4G , sheet_name='availability_auto', skiprows=3)

                 #df_availability_auto = sheets['availability_auto']
                 if "volume" in sheets:
                     # Lire avec skiprows pour ignorer les trois premières lignes si nécessaire
                     df_data_volume_auto = pd.read_excel(uploaded_file_4G , sheet_name='volume', skiprows=3)

                     #df_data_volume_auto = sheets['volume']

                     # Remplacer les en-têtes problématiques par des noms valides
                     replacements = {'#NAME?': 'Invalid_Name', '#N/A': 'Invalid_Name'}
                     for col in [df_availability_auto, df_data_volume_auto]:
                         col.columns = col.columns.str.replace(r'#NAME\?', 'Invalid_Name', regex=True)
                         col.columns = col.columns.str.replace(r'#N/A', 'Invalid_Name', regex=True)

                     # Renommer les colonnes de dates
                     for col in [df_availability_auto, df_data_volume_auto]:
                         date_columns = col.columns[2:16]
                         col.columns.values[2:16] = [f'Date_{i}' for i in range(1, len(date_columns) + 1)]

                     # Supprimer les colonnes non utiles
                     columns_to_drop = ['A', 'A.1', 'A.2', 'A.3', 'A.4', 
                                        'A.5','A.6','A.7','A.8',
                                        'A.9','A.10','A.11',
                                        'A.12','A.13','A.14',
                                        'A.15','A.16','A.17',
                                        'A.18','A.19','A.20',
                                        'A.21','A.22','A.23',
                                        'A.24']
                     # Supprimer les colonnes non utiles
                    #columns_to_drop = [f'A.{i}' for i in range(25)]
                     #for df in [df_availability_auto, df_data_volume_auto]:
                        #df.drop(columns=columns_to_drop, errors='ignore', inplace=True)

                     for col in [df_availability_auto, df_data_volume_auto]:
                         col.drop(columns=columns_to_drop, errors='ignore', inplace=True)

                     # Définir les colonnes à vérifier pour les valeurs nulles
                     columns_to_check = [f'Date_{i}' for i in range(1,15)]

                     # Filtrer les données 
                     availability_zero_df_auto = \
                         (df_availability_auto[(df_availability_auto[columns_to_check] == 
                         0).any(axis=1)])
                     
                     volume_zero_df_auto =(df_data_volume_auto[(df_data_volume_auto[columns_to_check] == 
                         0).any(axis=1)])

                     # Affichage des données filtrées pour la technologie 
                     st.subheader("Données filtrées (Disponibilité à zéro)")
                     
                     if not availability_zero_df_auto.empty:
                         st.write("Disponibilité 4G degradée:")
                         st.dataframe(availability_zero_df_auto)
                         
                         required_columns=['Unnamed: 0', 'Unnamed: 1','Date_12','Date_13','Date_14','site','region','action','cause']
                         
                         missing_columns=[col for col in required_columns if col not in availability_zero_df_auto.columns]
                         
                         if missing_columns:
                             st.warning(f"Les colonnes suivantes sont manquantes dans la disponibilité : {missing_columns}")
                         else:
                             availability_three_days_df= \
                             availability_zero_df_auto[required_columns]
                             st.write("disponibilité 4G degradée sur les trois derniers jours:")
                             st.dataframe(availability_three_days_df)    
                         
                     else:
                         st.write("Pas de données disponibles pour la disponibilité.")
                         
                     if not volume_zero_df_auto.empty:
                         st.write("volume degradée 4G:")
                         st.dataframe(volume_zero_df_auto)
                         
                         required_columns=['Unnamed: 0', 'Unnamed: 1','Date_12','Date_13','Date_14','site','region','action','cause']
                         
                         missing_columns=[col for col in required_columns if col not in volume_zero_df_auto.columns]
                         
                         if missing_columns:
                             st.warning(f"Les colonnes suivantes sont manquantes dans le volume : {missing_columns}")
                         else:
                             volume_three_days_df= \
                             volume_zero_df_auto[required_columns]
                             st.write("Données volume 4G sur les trois derniers jours:")
                             st.dataframe(volume_three_days_df)    
                         
                     else:
                         st.write("Pas de données disponibles pour le volume.")
                 else:
                     st.warning("La feuille de volume n'est pas présente dans le fichier.")


                     # Ajout du code pour analyser la disponibilité commune entre les réseaux

    if "df_availability_zero_2G" in locals() and "df_availability_zero_3G" in locals() and "availability_zero_df_auto" in locals():
        # Renommer les colonnes
        dispo2g = df_availability_zero_2G.rename(columns={'Unnamed: 0': 'cellule_2G', 'Unnamed: 1': 'Lob_2G'})
        dispo3g = df_availability_zero_3G.rename(columns={'Unnamed: 0': 'cellule_3G', 'Unnamed: 1': 'Lob_3G'})
        dispo4g = availability_zero_df_auto.rename(columns={'Unnamed: 0': 'cellule_4G', 'Unnamed: 1': 'cellules_4G'})

        # Créer des listes de sites
        liste1 = dispo2g['site'].to_list()
        liste2 = dispo3g['site'].to_list()
        liste3 = dispo4g['site'].to_list()

        # Trouver les sites communs
        sliste01 = set(liste1)
        sliste02 = set(liste2)
        sliste03 = set(liste3)
        commons = sliste01 & sliste02 & sliste03

        # Filtrer les DataFrames basés sur les sites communs
        data2g = dispo2g[dispo2g['site'].isin(commons)][['site', 'cellule_2G']]
        data3g = dispo3g[dispo3g['site'].isin(commons)][['site', 'cellule_3G']]
        data4g = dispo4g[dispo4g['site'].isin(commons)][['site', 'cellules_4G']]

        # Combiner les DataFrames
        data_combine = pd.merge(data2g, data3g, on='site', how='inner')
        data_final_combine = pd.merge(data_combine, data4g, on='site', how='inner')

        # Afficher les résultats dans Streamlit
        st.title("Analyse des sites communes avec des degradations")
        #st.write("Nombre de sites communs:", len(commons))
        
        #st.write("Données pour la 2G:")
        #st.dataframe(data2g)  
        
        #st.write("Données pour la 3G:")
        #st.dataframe(data3g)  
        
        #st.write("Données pour la 4G:")
        #st.dataframe(data4g)  

        # Afficher directement les données combinées dans l'interface de l'application
        st.write("sites communes ou la disponibilite est degradée:")
        st.dataframe(data_final_combine)  


# Vérifiez si les DataFrames nécessaires existent dans l'environnement local
if "df_data_zero_3G" in locals() and "volume_zero_df_auto" in locals():
    # Renommer les colonnes
    donnee3g = df_data_zero_3G.rename(columns={'Unnamed: 0': 'cellule_3G', 'Unnamed: 1': 'Lob_3G'})
    donnee4g = volume_zero_df_auto.rename(columns={'Unnamed: 0': 'cellule_4g', 'Unnamed: 1': 'cellule_4G'})

    # Créer des listes de sites
    liste2_data = donnee3g['site'].to_list()
    liste3_data = donnee4g['site'].to_list()

    # Trouver les sites communs
    sliste_02_data = set(liste2_data)
    sliste_03_data = set(liste3_data)
    commons_data = sliste_02_data & sliste_03_data

    # Afficher le nombre de sites communs
    #st.write("Nombre de sites communs:", len(commons_data))

    # Filtrer les DataFrames basés sur les sites communs
    data_data3g = donnee3g[donnee3g['site'].isin(commons_data)]
    data_data3g_use = data_data3g[['site', 'cellule_3G']]
    
    #st.write("Données pour la 3G:")
    #st.dataframe(data_data3g_use)
    
    print(data_data3g_use.shape)

    data_data4g = donnee4g[donnee4g['site'].isin(commons_data)]
    data_data4g_use = data_data4g[['site', 'cellule_4G']]
    
    #st.write("Données pour la 4G:")
    #st.dataframe(data_data4g_use)

    print(data_data4g_use.shape)

    # Fusionner les DataFrames filtrés
    data_final_combine = pd.merge(data_data3g_use, data_data4g_use, on='site', how='inner')

    # Vérifier les doublons dans le DataFrame final
    duplicated_count = data_final_combine.duplicated().sum()
    
    #st.write("Nombre de doublons dans les données combinées:", duplicated_count)

    # Liste des DataFrames à fusionner
    dfs = [data_data3g_use, data_data4g_use]

    # Fusionner tous les DataFrames en un seul
    final_data_indispon = reduce(lambda left, right: pd.merge(left, right, on='site', how='inner'), dfs)

    # Afficher les résultats finaux
    st.write("Les sites communes ou la data est degradée:")
    st.dataframe(final_data_indispon)

    
    # Renommer les colonnes
    voix2g = df_2G_trafic_zero.rename(columns={'Unnamed: 0': 'cellule_2G', 'Unnamed: 1': 'Lob_2G'})
    voix3g = df_voice_zero_3G.rename(columns={'Unnamed: 0': 'cellule_3G', 'Unnamed: 1': 'Lob_3G'})

    # Créer des listes de sites
    liste1_voice = voix2g['site'].to_list()
    liste2_voice = voix3g['site'].to_list()

    # Trouver les sites communs
    sliste_01_voice = set(liste1_voice)
    sliste_02_voice = set(liste2_voice)
    commons_voice = sliste_01_voice & sliste_02_voice

    # Afficher le nombre de sites communs
    #st.write("Nombre de sites communs:", len(commons_voice))

    # Filtrer les DataFrames basés sur les sites communs
    voice_data2g = voix2g[voix2g['site'].isin(commons_voice)]
    voice_data2g_use = voice_data2g[['site', 'cellule_2G']]
    
    #st.write("Données pour la 2G:")
    #st.dataframe(voice_data2g_use)
    
    print(voice_data2g_use.shape)

    voice_data3g = voix3g[voix3g['site'].isin(commons_voice)]
    voice_data3g_use = voice_data3g[['site', 'cellule_3G']]
    
    #st.write("Données pour la 3G:")
    #st.dataframe(voice_data3g_use)

    print(voice_data3g_use.shape)

    # Fusionner les DataFrames filtrés
    data_final_combine_voice = pd.merge(voice_data2g_use, voice_data3g_use, on='site', how='inner')

    # Vérifier les doublons dans le DataFrame final
    duplicated_count = data_final_combine_voice.duplicated().sum()
    
    #st.write("Nombre de doublons dans les données combinées:", duplicated_count)

    # Liste des DataFrames à fusionner
    dfs = [voice_data2g_use, voice_data3g_use]

    # Fusionner tous les DataFrames en un seul
    final_data_indispon_voice = reduce(lambda left, right: pd.merge(left, right, on='site', how='inner'), dfs)

    # Afficher les résultats finaux
    st.write("Les sites communes sur lesquelles la voix est degradee:")
    st.dataframe(final_data_indispon_voice)
