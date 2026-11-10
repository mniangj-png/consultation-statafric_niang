import pytest
from diagnostic.schema import FORMS

@pytest.fixture
def ins_answers():
    return {"QI01_Participation":1,"QI01_Pays":"Pays de test — fictif","QI01_INS":"INS fictif",
            "QI01_Statut":1,"QI02a":1,"QI02b":0,"QI03_Situation":1,"QI03_Mois":"10/2026",
            "QI04a":2,"QI04b":1,"QI05_Origine":1,"QI05_S1":1,"QI06a":[1,3],"QI06b":1,
            "QI07a":[1,2],"QI08_Situation":0,"QI09_D1":1,"QI10_P1":1,"QI12_Attribution":0}

@pytest.fixture
def minimal():
    return {key:{form['consent']:1,form['institution']:'Institution fictive de test'} for key,form in FORMS.items()}
