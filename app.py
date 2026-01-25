import streamlit as st
import pandas as pd
from datetime import datetime

# Configuration de la page
st.set_page_config(page_title="Consultation - Domaines Prioritaires", layout="centered")

# Titre et Introduction (basés sur votre document)
st.title("Questionnaire de Consultation")
st.markdown("""
**Guide de remplissage :**
Cette application vise à recueillir votre avis sur les domaines prioritaires.
Veuillez remplir les informations ci-dessous puis procéder à la sélection et au classement.
""")

# --- PARTIE 1 : IDENTIFICATION ---
st.header("1. Identification")
col1, col2 = st.columns(2)
with col1:
    nom = st.text_input("Nom")
    prenom = st.text_input("Prénom")
with col2:
    organisation = st.text_input("Organisation")
    # Liste abrégée pour l'exemple, à compléter avec tous les pays
    pays = st.selectbox("Pays de résidence", ["Sénégal", "Bénin", "Côte d'Ivoire", "Burkina Faso", "Autre"])

email = st.text_input("Adresse Email")

# --- PARTIE 2 : SÉLECTION DES DOMAINES (Q1) ---
st.header("2. Domaines Prioritaires")
st.info("Veuillez sélectionner au moins 5 domaines pertinents dans la liste ci-dessous.")

# Liste extraite de votre contexte habituel (à adapter selon le doc exact)
liste_domaines = [
    "Agriculture et sécurité alimentaire", "Santé publique", "Éducation et formation",
    "Gouvernance et démocratie", "Sécurité et stabilité", "Environnement et changement climatique",
    "Économie numérique", "Infrastructures", "Genre et inclusion sociale", "Culture et patrimoine"
]

selection_q1 = st.multiselect(
    "Quels sont les domaines qui nécessitent une intervention prioritaire ?",
    options=liste_domaines
)

if len(selection_q1) < 5:
    st.warning(f"Veuillez sélectionner encore au moins {5 - len(selection_q1)} domaines.")
else:
    st.success(f"{len(selection_q1)} domaines sélectionnés.")

    # --- PARTIE 3 : CLASSEMENT (TOP 5) ---
    st.header("3. Hiérarchisation (TOP 5)")
    st.markdown("Classez les 5 domaines les plus urgents parmi votre sélection.")

    # Logique dynamique : On ne peut classer que ce qu'on a sélectionné au-dessus
    top1 = st.selectbox("Priorité N°1", options=[""] + selection_q1)
    
    # On retire le choix 1 des options pour le choix 2, etc.
    rest_2 = [x for x in selection_q1 if x != top1]
    top2 = st.selectbox("Priorité N°2", options=[""] + rest_2)
    
    rest_3 = [x for x in rest_2 if x != top2]
    top3 = st.selectbox("Priorité N°3", options=[""] + rest_3)
    
    rest_4 = [x for x in rest_3 if x != top3]
    top4 = st.selectbox("Priorité N°4", options=[""] + rest_4)

    rest_5 = [x for x in rest_4 if x != top4]
    top5 = st.selectbox("Priorité N°5", options=[""] + rest_5)

    # --- PARTIE 4 : JUSTIFICATION ---
    st.header("4. Justification")
    justification = st.text_area("Pourquoi ce choix de priorités ? (Facultatif)")

    # --- ENREGISTREMENT ---
    if st.button("Soumettre mes réponses"):
        if top1 and top2 and top3 and top4 and top5:
            # Création de la ligne de données
            data = {
                "Date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "Nom": nom, "Prenom": prenom, "Email": email,
                "Organisation": organisation, "Pays": pays,
                "Selection_Complete": ", ".join(selection_q1),
                "TOP1": top1, "TOP2": top2, "TOP3": top3, "TOP4": top4, "TOP5": top5,
                "Justification": justification
            }
            
            # Sauvegarde dans un fichier CSV local (pour l'exemple)
            df = pd.DataFrame([data])
            try:
                # Ajoute à la suite du fichier existant sans réécrire l'en-tête
                df.to_csv("reponses_consultation.csv", mode='a', header=False, index=False)
            except FileNotFoundError:
                # Crée le fichier si n'existe pas
                df.to_csv("reponses_consultation.csv", mode='w', header=True, index=False)
            
            st.balloons()
            st.success("Merci ! Vos réponses ont été enregistrées avec succès.")
        else:
            st.error("Veuillez remplir intégralement le TOP 5 avant de soumettre.")