from __future__ import annotations

import base64
import csv
import io
import json
import os
import re
import secrets
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Tuple

import requests
import streamlit as st

APP_VERSION = "2026-04-13d"
RESPONSE_PATH_ROOT = "validation_doc"
DEFAULT_NOTE_URLS = {
    "en": os.getenv("NOTE_URL_EN", "https://1drv.ms/b/c/2afbae9640d93d5e/IQABX4McavudQZo7C7gOura-AdHesAay0yPW9kdPebcdl-k?e=gk6CIQ"),
    "fr": os.getenv("NOTE_URL_FR", "https://1drv.ms/b/c/2afbae9640d93d5e/IQCdAkqSoWp5Rr3QHGynDGoUAbasanwpp15xPR244J5qZzw?e=UNMnVs"),
    "pt": os.getenv("NOTE_URL_PT", "https://1drv.ms/b/c/2afbae9640d93d5e/IQDG4lcj85_2SIROmCoQgjhyAXEYfR5tZ92-G1MJjOHx2oI?e=oT297K"),
    "ar": os.getenv("NOTE_URL_AR", "https://1drv.ms/b/c/2afbae9640d93d5e/IQA6r00xFx6STI32n52sltbpAQw0AttwRuou4oaOe667b4M?e=7l5ocZ"),
}
DEFAULT_DOC_URL_EN = os.getenv(
    "FULL_DOC_URL_EN",
    "https://onedrive.live.com/personal/11cdb27337d4c5b5/_layouts/15/Doc.aspx?sourcedoc=%7Bbed3dcb8-d08f-45b8-9bba-17a454341159%7D&action=edit&redeem=aHR0cHM6Ly8xZHJ2Lm1zL3cvYy8xMWNkYjI3MzM3ZDRjNWI1L0lRQzQzTk8tajlDNFJadTZGNlJVTkJGWkFVTVpXMXUxWmozVjJQU2d4TUhvSTFFP2U9QUlyajJE"
)
DEFAULT_DOC_URL_FR = os.getenv(
    "FULL_DOC_URL_FR",
    "https://onedrive.live.com/personal/11cdb27337d4c5b5/_layouts/15/Doc.aspx?sourcedoc=%7Bbed3dcb8-d08f-45b8-9bba-17a454341159%7D&action=edit&redeem=aHR0cHM6Ly8xZHJ2Lm1zL3cvYy8xMWNkYjI3MzM3ZDRjNWI1L0lRQzQzTk8tajlDNFJadTZGNlJVTkJGWkFVTVpXMXUxWmozVjJQU2d4TUhvSTFFP2U9QUlyajJE"
)
DEFAULT_GITHUB_OWNER = os.getenv("GITHUB_OWNER", "mniangj-png")
DEFAULT_GITHUB_REPO = os.getenv("GITHUB_REPO", "consultation-statafric_niang")
DEFAULT_GITHUB_BRANCH = os.getenv("GITHUB_BRANCH", "data")

LANGUAGE_OPTIONS = {
    "en": "English",
    "fr": "Français",
    "pt": "Português",
    "ar": "العربية",
}

STEP_COUNT = 5
INSTITUTION_TYPES = ["nso", "rec"]
RESPONSE_CODES = ["go", "go_with_reservations", "no_go", "no_opinion"]
USABILITY_CODES = ["yes", "mostly_yes", "mostly_no", "no", "no_opinion"]
FINAL_POSITION_CODES = [
    "yes",
    "yes_limited_adjustments",
    "no_substantial_revision",
    "discuss_in_workshop",
    "no_opinion",
]

STRATEGIC_ROWS = [
    "strategic_prioritization_criteria",
    "strategic_scoring_logic",
    "strategic_core_extensions",
    "strategic_gender_integration",
    "strategic_min_disaggregations",
    "strategic_data_sources",
    "strategic_governance_roles",
    "strategic_roadmap_update",
]

DOMAIN_ROWS = [
    "domain_d01",
    "domain_d02",
    "domain_d03",
    "domain_d04",
    "domain_d05",
    "domain_d06",
    "domain_d07",
    "domain_d08",
    "domain_d09",
    "domain_d10",
    "domain_d11",
    "domain_d12",
]

RESPONDENT_TITLES = [
    "director_general",
    "statistician_general",
    "deputy_director_general",
    "director_statistics",
    "head_department",
    "programme_manager",
    "technical_expert",
    "other",
]

COUNTRY_OR_REC_OPTIONS = [
    "Algeria",
    "Angola",
    "Benin",
    "Botswana",
    "Burkina Faso",
    "Burundi",
    "Cabo Verde",
    "Cameroon",
    "Central African Republic",
    "Chad",
    "Comoros",
    "Congo",
    "Côte d’Ivoire",
    "Democratic Republic of the Congo",
    "Djibouti",
    "Egypt",
    "Equatorial Guinea",
    "Eritrea",
    "Eswatini",
    "Ethiopia",
    "Gabon",
    "Gambia",
    "Ghana",
    "Guinea",
    "Guinea-Bissau",
    "Kenya",
    "Lesotho",
    "Liberia",
    "Libya",
    "Madagascar",
    "Malawi",
    "Mali",
    "Mauritania",
    "Mauritius",
    "Morocco",
    "Mozambique",
    "Namibia",
    "Niger",
    "Nigeria",
    "Rwanda",
    "Sahrawi Arab Democratic Republic",
    "Sao Tome and Principe",
    "Senegal",
    "Seychelles",
    "Sierra Leone",
    "Somalia",
    "South Africa",
    "South Sudan",
    "Sudan",
    "Tanzania",
    "Togo",
    "Tunisia",
    "Uganda",
    "Zambia",
    "Zimbabwe",
    "AMU",
    "CEN-SAD",
    "COMESA",
    "EAC",
    "ECCAS",
    "ECOWAS",
    "IGAD",
    "SADC",
    "Other",
]

TRANSLATIONS: Dict[str, Dict] = {
    "en": {
        "title": "Strategic validation of the draft document on priority socio-economic statistics in Africa",
        "subtitle": "Multilingual institutional questionnaire built from the decision-oriented summary note.",
        "doc_links": "Full draft document",
        "note_downloads": "Decision-oriented summary note",
        "note_en": "Summary note - English",
        "note_fr": "Summary note - French",
        "note_pt": "Summary note - Portuguese (Portugal)",
        "note_ar": "Summary note - Arabic",
        "lang": "Language",
        "intro": "Please complete this questionnaire on behalf of your institution.",
        "intro2": "It collects a structured institutional position on the main strategic choices of the draft document on priority socio-economic statistics in Africa, with due consideration of the gender dimension.",
        "intro3": "It complements the summary note and the full draft document. It does not replace a detailed review, but it helps secure rapid institutional Go/No-Go decisions on the main methodological, thematic, and operational choices.",
        "intro4": "Please respond after internal consultation whenever possible.",
        "scale_title": "Response scale used",
        "scale": [
            "Go: overall agreement",
            "Go with reservations: agreement subject to limited adjustments",
            "No-Go: major revision requested",
            "No opinion: no position at this stage",
        ],
        "estimated": "Estimated completion time: 10 to 12 minutes.",
        "step_label": "Progress",
        "ref_docs": "Reference documents",
        "note_link": "Decision-oriented summary note",
        "doc_en": "English version",
        "doc_fr": "French version",
        "save_draft": "Save draft and pause for 48 hours",
        "load_draft": "Resume a saved draft",
        "draft_code": "Draft code",
        "draft_code_placeholder": "Paste your draft code",
        "draft_saved": "Draft saved successfully. Keep this code to resume within 48 hours.",
        "draft_loaded": "Draft loaded successfully.",
        "draft_expired": "This draft has expired.",
        "draft_missing": "Draft not found.",
        "draft_download": "Download current draft (JSON)",
        "response_download_json": "Download current response (JSON)",
        "response_download_csv": "Download current response (CSV)",
        "sidebar_help": "You may save a partial draft and resume it for up to 48 hours.",
        "sidebar_repo_missing": "GitHub saving is not configured. You can still download your draft locally.",
        "validation_title": "Please complete the required fields before continuing.",
        "back": "Back",
        "continue": "Continue",
        "submit": "Submit final response",
        "start_over": "Start a new response",
        "submit_success": "Thank you. Your final response has been recorded successfully.",
        "submit_warning": "The form is valid, but online saving failed. Please download the response files below and share them manually.",
        "draft_code_note": "Resume code",
        "questions_required": "Required questions must be completed to continue.",
        "save_label": "Saving…",
        "overall_why": "Please explain why",
        "optional_summary": "Additional summary comments (optional)",
        "optional_revisions": "What are the three most important revisions needed before final validation? (optional)",
        "other_specify": "Please specify",
        "other_country": "Other country or REC",
        "sections": {
            1: "Section 1. Identification of the respondent",
            2: "Section 2. Overall validation",
            3: "Section 3. Validation of strategic choices",
            4: "Section 4. Validation of thematic domains",
            5: "Section 5. Final institutional position",
        },
        "questions": {
            "institution_acronym": "1. Acronym of institution",
            "institution_type": "2. Type of institution",
            "country_or_rec": "3. Country or REC represented",
            "respondent_title": "4. Title of main respondent",
            "email": "5. Email",
            "overall_validation": "6. Overall validation of the document",
            "operational_usability": "7. Is the document operational enough for use by Member States and RECs?",
            "strategic_grid": "8. Please assess the following strategic elements",
            "strategic_comments": "9. If you selected ‘Go with reservations’ or ‘No-Go’ for one or more elements above, please specify which ones and why",
            "domain_grid": "10. Please assess the 12 proposed thematic domains",
            "domain_comments": "11. If you selected ‘Go with reservations’ or ‘No-Go’ for one or more domains, please specify which ones and why",
            "top_3_revisions": "12. What are the three most important revisions needed before final validation?",
            "final_position": "13. Is your institution broadly in favor of finalizing the document after consideration of comments received?",
        },
        "institution_types": {
            "nso": "National Statistical Office",
            "rec": "Regional Economic Community",
        },
        "responses": {
            "go": "Go",
            "go_with_reservations": "Go with reservations",
            "no_go": "No-Go",
            "no_opinion": "No opinion",
        },
        "usability": {
            "yes": "Yes",
            "mostly_yes": "Mostly yes",
            "mostly_no": "Mostly no",
            "no": "No",
            "no_opinion": "No opinion",
        },
        "final_positions": {
            "yes": "Yes",
            "yes_limited_adjustments": "Yes, subject to limited adjustments",
            "no_substantial_revision": "No, more substantial revision is needed",
            "discuss_in_workshop": "To be discussed in a workshop",
            "no_opinion": "No opinion",
        },
        "titles": {
            "director_general": "Director General",
            "statistician_general": "Statistician General",
            "deputy_director_general": "Deputy Director General",
            "director_statistics": "Director of Statistics",
            "head_department": "Head of department / unit",
            "programme_manager": "Programme manager / coordinator",
            "technical_expert": "Technical expert / statistician",
            "other": "Other",
        },
        "strategic_rows": {
            "strategic_prioritization_criteria": "Prioritization criteria used",
            "strategic_scoring_logic": "Multi-criteria scoring logic",
            "strategic_core_extensions": "Core and extensions logic",
            "strategic_gender_integration": "Cross-cutting integration of gender",
            "strategic_min_disaggregations": "Proposed minimum disaggregations",
            "strategic_data_sources": "Data sources and production arrangements",
            "strategic_governance_roles": "Governance and allocation of roles",
            "strategic_roadmap_update": "Implementation roadmap and updating mechanism",
        },
        "domain_rows": {
            "domain_d01": "D01 Economic growth, structural transformation and trade",
            "domain_d02": "D02 Employment, decent work and social protection",
            "domain_d03": "D03 Sustainable agriculture, food security and nutrition",
            "domain_d04": "D04 Infrastructure, industrialization and innovation",
            "domain_d05": "D05 Inclusion, poverty and inequality",
            "domain_d06": "D06 Education, skills and human capital",
            "domain_d07": "D07 Health, well-being and universal access",
            "domain_d08": "D08 Gender equality and empowerment",
            "domain_d09": "D09 Environment, climate resilience and sustainable cities",
            "domain_d10": "D10 Governance, peace and institutions",
            "domain_d11": "D11 Blue economy and ocean management",
            "domain_d12": "D12 Partnerships and development financing",
        },
        "placeholders": {
            "institution_acronym": "e.g. ANSD / COMESA",
            "email": "name@institution.org",
            "why": "Brief justification",
            "summary": "Optional summary comments",
            "revisions": "List the three most important revisions, if any",
        },
    },
    "fr": {
        "title": "Validation stratégique du projet de document sur les statistiques socio-économiques prioritaires en Afrique",
        "subtitle": "Questionnaire institutionnel multilingue construit à partir de la note de synthèse décisionnelle.",
        "doc_links": "Document complet",
        "note_downloads": "Note de synthèse décisionnelle",
        "note_en": "Note - version anglaise",
        "note_fr": "Note - version française",
        "note_pt": "Note - version portugaise (Portugal)",
        "note_ar": "Note - version arabe",
        "lang": "Langue",
        "intro": "Merci de renseigner ce questionnaire au nom de votre institution.",
        "intro2": "Il vise à recueillir une position institutionnelle structurée sur les principaux choix stratégiques du projet de document relatif à l’identification des statistiques socio-économiques prioritaires en Afrique, avec prise en compte de la dimension genre.",
        "intro3": "Il complète la note de synthèse et le document complet. Il ne remplace pas une relecture détaillée, mais permet de sécuriser rapidement les arbitrages institutionnels Go / No-Go sur les principaux choix méthodologiques, thématiques et opérationnels.",
        "intro4": "Merci de répondre, dans la mesure du possible, après concertation interne.",
        "scale_title": "Échelle de réponse utilisée",
        "scale": [
            "Validé : accord global",
            "Validé sous réserve : accord sous ajustements limités",
            "Non-validé : révision importante demandée",
            "Sans avis : pas de position à ce stade",
        ],
        "estimated": "Temps estimé de réponse : 10 à 12 minutes.",
        "step_label": "Progression",
        "ref_docs": "Documents de référence",
        "note_link": "Note de synthèse décisionnelle",
        "doc_en": "Version anglaise",
        "doc_fr": "Version française",
        "save_draft": "Enregistrer le brouillon et suspendre pendant 48 heures",
        "load_draft": "Reprendre un brouillon enregistré",
        "draft_code": "Code du brouillon",
        "draft_code_placeholder": "Collez votre code de brouillon",
        "draft_saved": "Brouillon enregistré avec succès. Conservez ce code pour reprendre dans les 48 heures.",
        "draft_loaded": "Brouillon chargé avec succès.",
        "draft_expired": "Ce brouillon a expiré.",
        "draft_missing": "Brouillon introuvable.",
        "draft_download": "Télécharger le brouillon courant (JSON)",
        "response_download_json": "Télécharger la réponse courante (JSON)",
        "response_download_csv": "Télécharger la réponse courante (CSV)",
        "sidebar_help": "Vous pouvez enregistrer un brouillon partiel et le reprendre pendant 48 heures.",
        "sidebar_repo_missing": "L’enregistrement GitHub n’est pas configuré. Vous pouvez toutefois télécharger votre brouillon localement.",
        "validation_title": "Merci de renseigner les champs obligatoires avant de poursuivre.",
        "back": "Retour",
        "continue": "Continuer",
        "submit": "Soumettre la réponse finale",
        "start_over": "Commencer une nouvelle réponse",
        "submit_success": "Merci. Votre réponse finale a été enregistrée avec succès.",
        "submit_warning": "Le formulaire est valide, mais l’enregistrement en ligne a échoué. Merci de télécharger les fichiers de réponse ci-dessous et de les transmettre manuellement.",
        "draft_code_note": "Code de reprise",
        "questions_required": "Les questions obligatoires doivent être renseignées pour pouvoir poursuivre.",
        "save_label": "Enregistrement…",
        "overall_why": "Merci de préciser pourquoi",
        "optional_summary": "Commentaires de synthèse supplémentaires (facultatif)",
        "optional_revisions": "Quelles sont les trois révisions les plus importantes à apporter avant validation finale ? (facultatif)",
        "other_specify": "Merci de préciser",
        "other_country": "Autre pays ou CER",
        "sections": {
            1: "Section 1. Identification du répondant",
            2: "Section 2. Validation générale",
            3: "Section 3. Validation des choix stratégiques",
            4: "Section 4. Validation des domaines thématiques",
            5: "Section 5. Position finale de l’institution",
        },
        "questions": {
            "institution_acronym": "1. Sigle de l’institution",
            "institution_type": "2. Type d’institution",
            "country_or_rec": "3. Pays ou CER représentés",
            "respondent_title": "4. Fonction du répondant principal",
            "email": "5. Email",
            "overall_validation": "6. Validation globale du document",
            "operational_usability": "7. Le document vous paraît-il suffisamment opérationnel pour un usage par les États membres et les CER ?",
            "strategic_grid": "8. Veuillez apprécier les éléments stratégiques suivants",
            "strategic_comments": "9. Si vous avez indiqué ‘Validé sous réserve’ ou ‘Non-validé’ pour un ou plusieurs éléments ci-dessus, merci de préciser lesquels et pourquoi",
            "domain_grid": "10. Veuillez apprécier les 12 domaines thématiques proposés",
            "domain_comments": "11. Si vous avez indiqué ‘Validé sous réserve’ ou ‘Non-validé’ pour un ou plusieurs domaines, merci de préciser lesquels et pourquoi",
            "top_3_revisions": "12. Quelles sont les trois révisions les plus importantes à apporter avant validation finale ?",
            "final_position": "13. Votre institution est-elle globalement favorable à la finalisation du document après prise en compte des observations reçues ?",
        },
        "institution_types": {
            "nso": "Institut national de statistique (INS)",
            "rec": "Communauté économique régionale (CER)",
        },
        "responses": {
            "go": "Validé",
            "go_with_reservations": "Validé sous réserve",
            "no_go": "Non-validé",
            "no_opinion": "Sans avis",
        },
        "usability": {
            "yes": "Oui",
            "mostly_yes": "Plutôt oui",
            "mostly_no": "Plutôt non",
            "no": "Non",
            "no_opinion": "Sans avis",
        },
        "final_positions": {
            "yes": "Oui",
            "yes_limited_adjustments": "Oui, sous réserve d’ajustements limités",
            "no_substantial_revision": "Non, une révision plus substantielle est nécessaire",
            "discuss_in_workshop": "À discuter en atelier",
            "no_opinion": "Sans avis",
        },
        "titles": {
            "director_general": "Directeur général",
            "statistician_general": "Statisticien général",
            "deputy_director_general": "Directeur général adjoint",
            "director_statistics": "Directeur des statistiques",
            "head_department": "Chef de département / unité",
            "programme_manager": "Responsable / coordonnateur de programme",
            "technical_expert": "Expert technique / statisticien",
            "other": "Autre",
        },
        "strategic_rows": {
            "strategic_prioritization_criteria": "Critères de priorisation retenus",
            "strategic_scoring_logic": "Logique de notation multicritère (scoring)",
            "strategic_core_extensions": "Distinction noyau / extensions",
            "strategic_gender_integration": "Intégration transversale du genre",
            "strategic_min_disaggregations": "Désagrégations minimales proposées",
            "strategic_data_sources": "Sources de données et dispositifs de production",
            "strategic_governance_roles": "Gouvernance et répartition des rôles",
            "strategic_roadmap_update": "Feuille de route de mise en œuvre et mécanisme de mise à jour",
        },
        "domain_rows": {
            "domain_d01": "D01 Croissance économique, transformation structurelle et commerce",
            "domain_d02": "D02 Emploi, travail décent et protection sociale",
            "domain_d03": "D03 Agriculture durable, sécurité alimentaire et nutrition",
            "domain_d04": "D04 Infrastructures, industrialisation et innovation",
            "domain_d05": "D05 Inclusion, pauvreté et inégalités",
            "domain_d06": "D06 Éducation, compétences et capital humain",
            "domain_d07": "D07 Santé, bien-être et accès universel",
            "domain_d08": "D08 Égalité des genres et autonomisation",
            "domain_d09": "D09 Environnement, résilience climatique et villes durables",
            "domain_d10": "D10 Gouvernance, paix et institutions",
            "domain_d11": "D11 Économie bleue et gestion des océans",
            "domain_d12": "D12 Partenariats et financement du développement",
        },
        "placeholders": {
            "institution_acronym": "Ex. ANSD / COMESA",
            "email": "nom@institution.org",
            "why": "Brève justification",
            "summary": "Commentaires de synthèse facultatifs",
            "revisions": "Listez, le cas échéant, les trois révisions les plus importantes",
        },
    },
    "pt": {
        "title": "Validação estratégica do projeto de documento sobre estatísticas socioeconómicas prioritárias em África",
        "subtitle": "Questionário institucional multilingue construído a partir da nota de síntese decisional.",
        "lang": "Idioma",
        "intro": "Por favor, preencha este questionário em nome da sua instituição.",
        "intro2": "O objetivo é recolher uma posição institucional estruturada sobre as principais escolhas estratégicas do projeto de documento relativo à identificação das estatísticas socioeconómicas prioritárias em África, com consideração da dimensão de género.",
        "intro3": "Complementa a nota de síntese e o documento completo. Não substitui uma revisão detalhada, mas ajuda a assegurar rapidamente decisões institucionais Go / No-Go sobre as principais escolhas metodológicas, temáticas e operacionais.",
        "intro4": "Responda, se possível, após concertação interna.",
        "scale_title": "Escala de resposta utilizada",
        "scale": [
            "Validado: acordo geral",
            "Validado com reservas: acordo sujeito a ajustamentos limitados",
            "Não validado: revisão importante solicitada",
            "Sem opinião: sem posição nesta fase",
        ],
        "estimated": "Tempo estimado de resposta: 10 a 12 minutos.",
        "step_label": "Progresso",
        "ref_docs": "Documentos de referência",
        "note_link": "Nota de síntese decisional",
        "doc_en": "Versão inglesa",
        "doc_fr": "Versão francesa",
        "save_draft": "Guardar rascunho e suspender durante 48 horas",
        "load_draft": "Retomar um rascunho guardado",
        "draft_code": "Código do rascunho",
        "draft_code_placeholder": "Cole o seu código de rascunho",
        "draft_saved": "Rascunho guardado com sucesso. Guarde este código para retomar dentro de 48 horas.",
        "draft_loaded": "Rascunho carregado com sucesso.",
        "draft_expired": "Este rascunho expirou.",
        "draft_missing": "Rascunho não encontrado.",
        "draft_download": "Descarregar rascunho atual (JSON)",
        "response_download_json": "Descarregar resposta atual (JSON)",
        "response_download_csv": "Descarregar resposta atual (CSV)",
        "sidebar_help": "Pode guardar um rascunho parcial e retomá-lo durante 48 horas.",
        "sidebar_repo_missing": "A gravação no GitHub não está configurada. Ainda assim, pode descarregar o rascunho localmente.",
        "validation_title": "Preencha os campos obrigatórios antes de continuar.",
        "back": "Voltar",
        "continue": "Continuar",
        "submit": "Submeter a resposta final",
        "start_over": "Iniciar uma nova resposta",
        "submit_success": "Obrigado. A sua resposta final foi registada com sucesso.",
        "submit_warning": "O formulário é válido, mas a gravação online falhou. Descarregue os ficheiros de resposta abaixo e partilhe-os manualmente.",
        "draft_code_note": "Código de retoma",
        "questions_required": "As perguntas obrigatórias devem ser preenchidas para continuar.",
        "save_label": "A guardar…",
        "overall_why": "Por favor, explique porquê",
        "optional_summary": "Comentários adicionais de síntese (opcional)",
        "optional_revisions": "Quais são as três revisões mais importantes a introduzir antes da validação final? (opcional)",
        "other_specify": "Por favor, especifique",
        "other_country": "Outro país ou CER",
        "sections": {
            1: "Secção 1. Identificação do respondente",
            2: "Secção 2. Validação geral",
            3: "Secção 3. Validação das escolhas estratégicas",
            4: "Secção 4. Validação dos domínios temáticos",
            5: "Secção 5. Posição final da instituição",
        },
        "questions": {
            "institution_acronym": "1. Sigla da instituição",
            "institution_type": "2. Tipo de instituição",
            "country_or_rec": "3. País ou CER representado",
            "respondent_title": "4. Função do principal respondente",
            "email": "5. Email",
            "overall_validation": "6. Validação global do documento",
            "operational_usability": "7. O documento parece suficientemente operacional para utilização pelos Estados-Membros e pelas CER?",
            "strategic_grid": "8. Aprecie os seguintes elementos estratégicos",
            "strategic_comments": "9. Se selecionou ‘Validado com reservas’ ou ‘Não validado’ para um ou mais elementos acima, especifique quais e porquê",
            "domain_grid": "10. Aprecie os 12 domínios temáticos propostos",
            "domain_comments": "11. Se selecionou ‘Validado com reservas’ ou ‘Não validado’ para um ou mais domínios, especifique quais e porquê",
            "top_3_revisions": "12. Quais são as três revisões mais importantes a introduzir antes da validação final?",
            "final_position": "13. A sua instituição é globalmente favorável à finalização do documento após a consideração das observações recebidas?",
        },
        "institution_types": {
            "nso": "Instituto Nacional de Estatística",
            "rec": "Comunidade Económica Regional",
        },
        "responses": {
            "go": "Validado",
            "go_with_reservations": "Validado com reservas",
            "no_go": "Não validado",
            "no_opinion": "Sem opinião",
        },
        "usability": {
            "yes": "Sim",
            "mostly_yes": "Em grande medida sim",
            "mostly_no": "Em grande medida não",
            "no": "Não",
            "no_opinion": "Sem opinião",
        },
        "final_positions": {
            "yes": "Sim",
            "yes_limited_adjustments": "Sim, sujeito a ajustamentos limitados",
            "no_substantial_revision": "Não, é necessária uma revisão mais substancial",
            "discuss_in_workshop": "A discutir em atelier",
            "no_opinion": "Sem opinião",
        },
        "titles": {
            "director_general": "Diretor-geral",
            "statistician_general": "Estatístico-geral",
            "deputy_director_general": "Diretor-geral adjunto",
            "director_statistics": "Diretor de estatística",
            "head_department": "Chefe de departamento / unidade",
            "programme_manager": "Gestor / coordenador de programa",
            "technical_expert": "Perito técnico / estatístico",
            "other": "Outro",
        },
        "strategic_rows": {
            "strategic_prioritization_criteria": "Critérios de priorização adotados",
            "strategic_scoring_logic": "Lógica de pontuação multicritério",
            "strategic_core_extensions": "Distinção núcleo / extensões",
            "strategic_gender_integration": "Integração transversal do género",
            "strategic_min_disaggregations": "Desagregações mínimas propostas",
            "strategic_data_sources": "Fontes de dados e dispositivos de produção",
            "strategic_governance_roles": "Governação e repartição de papéis",
            "strategic_roadmap_update": "Roteiro de implementação e mecanismo de atualização",
        },
        "domain_rows": {
            "domain_d01": "D01 Crescimento económico, transformação estrutural e comércio",
            "domain_d02": "D02 Emprego, trabalho decente e proteção social",
            "domain_d03": "D03 Agricultura sustentável, segurança alimentar e nutrição",
            "domain_d04": "D04 Infraestruturas, industrialização e inovação",
            "domain_d05": "D05 Inclusão, pobreza e desigualdades",
            "domain_d06": "D06 Educação, competências e capital humano",
            "domain_d07": "D07 Saúde, bem-estar e acesso universal",
            "domain_d08": "D08 Igualdade de género e empoderamento",
            "domain_d09": "D09 Ambiente, resiliência climática e cidades sustentáveis",
            "domain_d10": "D10 Governação, paz e instituições",
            "domain_d11": "D11 Economia azul e gestão dos oceanos",
            "domain_d12": "D12 Parcerias e financiamento do desenvolvimento",
        },
        "placeholders": {
            "institution_acronym": "Ex. INE / COMESA",
            "email": "nome@instituicao.org",
            "why": "Justificação breve",
            "summary": "Comentários de síntese opcionais",
            "revisions": "Indique, se for o caso, as três revisões mais importantes",
        },
    },
    "ar": {
        "title": "التحقق الاستراتيجي من مشروع الوثيقة الخاصة بالإحصاءات الاجتماعية والاقتصادية ذات الأولوية في أفريقيا",
        "subtitle": "استبيان مؤسسي متعدد اللغات مبني على المذكرة التركيبية التقريرية.",
        "doc_links": "الوثيقة الكاملة",
        "note_downloads": "المذكرة التركيبية التقريرية",
        "note_en": "المذكرة - النسخة الإنجليزية",
        "note_fr": "المذكرة - النسخة الفرنسية",
        "note_pt": "المذكرة - النسخة البرتغالية (البرتغال)",
        "note_ar": "المذكرة - النسخة العربية",
        "lang": "اللغة",
        "intro": "يرجى ملء هذا الاستبيان باسم مؤسستكم.",
        "intro2": "يهدف إلى جمع موقف مؤسسي منظم بشأن الخيارات الاستراتيجية الرئيسية في مشروع الوثيقة المتعلقة بتحديد الإحصاءات الاجتماعية والاقتصادية ذات الأولوية في أفريقيا، مع مراعاة بُعد النوع الاجتماعي.",
        "intro3": "يكمّل المذكرة التركيبية والوثيقة الكاملة. وهو لا يحل محل المراجعة التفصيلية، لكنه يساعد على تأمين قرارات مؤسسية سريعة من نوع Go / No-Go بشأن الخيارات المنهجية والموضوعية والتنفيذية الرئيسية.",
        "intro4": "يرجى الإجابة، قدر الإمكان، بعد التشاور الداخلي.",
        "scale_title": "مقياس الإجابة المستخدم",
        "scale": [
            "مُعتمد: موافقة عامة",
            "مُعتمد مع تحفظات: موافقة مع تعديلات محدودة",
            "غير معتمد: مطلوب تنقيح مهم",
            "لا رأي: لا يوجد موقف في هذه المرحلة",
        ],
        "estimated": "الوقت التقديري للإجابة: من 10 إلى 12 دقيقة.",
        "step_label": "التقدم",
        "ref_docs": "الوثائق المرجعية",
        "note_link": "المذكرة التركيبية التقريرية",
        "doc_en": "النسخة الإنجليزية",
        "doc_fr": "النسخة الفرنسية",
        "save_draft": "حفظ المسودة وإيقافها لمدة 48 ساعة",
        "load_draft": "استئناف مسودة محفوظة",
        "draft_code": "رمز المسودة",
        "draft_code_placeholder": "ألصق رمز المسودة",
        "draft_saved": "تم حفظ المسودة بنجاح. احتفظ بهذا الرمز للاستئناف خلال 48 ساعة.",
        "draft_loaded": "تم تحميل المسودة بنجاح.",
        "draft_expired": "انتهت صلاحية هذه المسودة.",
        "draft_missing": "المسودة غير موجودة.",
        "draft_download": "تنزيل المسودة الحالية (JSON)",
        "response_download_json": "تنزيل الإجابة الحالية (JSON)",
        "response_download_csv": "تنزيل الإجابة الحالية (CSV)",
        "sidebar_help": "يمكنكم حفظ مسودة جزئية واستئنافها خلال 48 ساعة.",
        "sidebar_repo_missing": "حفظ GitHub غير مُعدّ. ومع ذلك يمكن تنزيل المسودة محلياً.",
        "validation_title": "يرجى استكمال الحقول الإلزامية قبل المتابعة.",
        "back": "رجوع",
        "continue": "متابعة",
        "submit": "إرسال الإجابة النهائية",
        "start_over": "بدء إجابة جديدة",
        "submit_success": "شكراً لكم. تم تسجيل الإجابة النهائية بنجاح.",
        "submit_warning": "الاستبيان صالح، لكن الحفظ عبر الإنترنت فشل. يرجى تنزيل ملفات الإجابة أدناه ومشاركتها يدوياً.",
        "draft_code_note": "رمز الاستئناف",
        "questions_required": "يجب استكمال الأسئلة الإلزامية للمتابعة.",
        "save_label": "جارٍ الحفظ…",
        "overall_why": "يرجى توضيح السبب",
        "optional_summary": "ملاحظات تلخيصية إضافية (اختياري)",
        "optional_revisions": "ما هي أهم ثلاثة تعديلات مطلوبة قبل الاعتماد النهائي؟ (اختياري)",
        "other_specify": "يرجى التحديد",
        "other_country": "بلد أو تجمع إقليمي آخر",
        "sections": {
            1: "القسم 1. تعريف المجيب",
            2: "القسم 2. التحقق العام",
            3: "القسم 3. التحقق من الخيارات الاستراتيجية",
            4: "القسم 4. التحقق من المجالات الموضوعية",
            5: "القسم 5. الموقف النهائي للمؤسسة",
        },
        "questions": {
            "institution_acronym": "1. اختصار المؤسسة",
            "institution_type": "2. نوع المؤسسة",
            "country_or_rec": "3. البلد أو التجمع الإقليمي الممثل",
            "respondent_title": "4. صفة المجيب الرئيسي",
            "email": "5. البريد الإلكتروني",
            "overall_validation": "6. التحقق العام من الوثيقة",
            "operational_usability": "7. هل تبدو الوثيقة عملية بما يكفي للاستخدام من قبل الدول الأعضاء والتجمعات الاقتصادية الإقليمية؟",
            "strategic_grid": "8. يرجى تقييم العناصر الاستراتيجية التالية",
            "strategic_comments": "9. إذا اخترتم ‘مُعتمد مع تحفظات’ أو ‘غير معتمد’ لعنصر واحد أو أكثر أعلاه، يرجى تحديد العناصر وسبب ذلك",
            "domain_grid": "10. يرجى تقييم المجالات الموضوعية الاثني عشر المقترحة",
            "domain_comments": "11. إذا اخترتم ‘مُعتمد مع تحفظات’ أو ‘غير معتمد’ لمجال واحد أو أكثر، يرجى تحديد المجالات وسبب ذلك",
            "top_3_revisions": "12. ما هي أهم ثلاثة تعديلات مطلوبة قبل الاعتماد النهائي؟",
            "final_position": "13. هل مؤسستكم مؤيدة عموماً لإنهاء الوثيقة بعد أخذ الملاحظات الواردة في الاعتبار؟",
        },
        "institution_types": {
            "nso": "المعهد الوطني للإحصاء",
            "rec": "التجمع الاقتصادي الإقليمي",
        },
        "responses": {
            "go": "مُعتمد",
            "go_with_reservations": "مُعتمد مع تحفظات",
            "no_go": "غير معتمد",
            "no_opinion": "لا رأي",
        },
        "usability": {
            "yes": "نعم",
            "mostly_yes": "نعم إلى حد كبير",
            "mostly_no": "لا إلى حد كبير",
            "no": "لا",
            "no_opinion": "لا رأي",
        },
        "final_positions": {
            "yes": "نعم",
            "yes_limited_adjustments": "نعم، مع تعديلات محدودة",
            "no_substantial_revision": "لا، يلزم تنقيح أكثر جوهرية",
            "discuss_in_workshop": "يُناقش في ورشة عمل",
            "no_opinion": "لا رأي",
        },
        "titles": {
            "director_general": "المدير العام",
            "statistician_general": "الإحصائي العام",
            "deputy_director_general": "نائب المدير العام",
            "director_statistics": "مدير الإحصاءات",
            "head_department": "رئيس الإدارة / الوحدة",
            "programme_manager": "مسؤول / منسق البرنامج",
            "technical_expert": "خبير تقني / إحصائي",
            "other": "أخرى",
        },
        "strategic_rows": {
            "strategic_prioritization_criteria": "معايير تحديد الأولويات المعتمدة",
            "strategic_scoring_logic": "منطق التنقيط متعدد المعايير",
            "strategic_core_extensions": "التمييز بين النواة والامتدادات",
            "strategic_gender_integration": "الإدماج العرضي للنوع الاجتماعي",
            "strategic_min_disaggregations": "التفصيلات الدنيا المقترحة",
            "strategic_data_sources": "مصادر البيانات وترتيبات الإنتاج",
            "strategic_governance_roles": "الحوكمة وتوزيع الأدوار",
            "strategic_roadmap_update": "خارطة الطريق وآلية التحيين",
        },
        "domain_rows": {
            "domain_d01": "D01 النمو الاقتصادي والتحول الهيكلي والتجارة",
            "domain_d02": "D02 التشغيل والعمل اللائق والحماية الاجتماعية",
            "domain_d03": "D03 الزراعة المستدامة والأمن الغذائي والتغذية",
            "domain_d04": "D04 البنية التحتية والتصنيع والابتكار",
            "domain_d05": "D05 الإدماج والفقر وعدم المساواة",
            "domain_d06": "D06 التعليم والمهارات ورأس المال البشري",
            "domain_d07": "D07 الصحة والرفاه والولوج الشامل",
            "domain_d08": "D08 المساواة بين الجنسين والتمكين",
            "domain_d09": "D09 البيئة والقدرة على الصمود المناخي والمدن المستدامة",
            "domain_d10": "D10 الحوكمة والسلام والمؤسسات",
            "domain_d11": "D11 الاقتصاد الأزرق وتدبير المحيطات",
            "domain_d12": "D12 الشراكات وتمويل التنمية",
        },
        "placeholders": {
            "institution_acronym": "مثال: ANSD / COMESA",
            "email": "name@institution.org",
            "why": "مبرر موجز",
            "summary": "ملاحظات تلخيصية اختيارية",
            "revisions": "اذكر، عند الاقتضاء، أهم ثلاثة تعديلات",
        },
    },
}

FIELD_DEFAULTS = {
    "institution_acronym": "",
    "institution_type": None,
    "country_or_rec": None,
    "country_or_rec_other": "",
    "respondent_title": None,
    "respondent_title_other": "",
    "email": "",
    "overall_validation": None,
    "overall_validation_why": "",
    "operational_usability": None,
    "operational_usability_why": "",
    "strategic_comments": "",
    "domain_comments": "",
    "top_3_revisions": "",
    "final_institutional_position": None,
    "draft_code": "",
}

for row in STRATEGIC_ROWS + DOMAIN_ROWS:
    FIELD_DEFAULTS[row] = None
    FIELD_DEFAULTS[f"{row}_why"] = ""

STEP_FIELDS = {
    1: [
        "institution_acronym",
        "institution_type",
        "country_or_rec",
        "country_or_rec_other",
        "respondent_title",
        "respondent_title_other",
        "email",
    ],
    2: [
        "overall_validation",
        "overall_validation_why",
        "operational_usability",
        "operational_usability_why",
    ],
    3: STRATEGIC_ROWS + [f"{row}_why" for row in STRATEGIC_ROWS] + ["strategic_comments"],
    4: DOMAIN_ROWS + [f"{row}_why" for row in DOMAIN_ROWS] + ["domain_comments", "top_3_revisions"],
    5: ["final_institutional_position"],
}


def ensure_form_data() -> None:
    if "form_data" not in st.session_state:
        st.session_state.form_data = dict(FIELD_DEFAULTS)
    else:
        for key, value in FIELD_DEFAULTS.items():
            st.session_state.form_data.setdefault(key, value)


def get_value(key: str):
    ensure_form_data()
    return st.session_state.form_data.get(key, FIELD_DEFAULTS.get(key))


def set_value(key: str, value) -> None:
    ensure_form_data()
    st.session_state.form_data[key] = value


def widget_key(field: str) -> str:
    return f"w_{field}"


def prime_widget(field: str) -> str:
    wk = widget_key(field)
    if wk not in st.session_state:
        st.session_state[wk] = get_value(field)
    return wk


def sync_fields(fields: list[str]) -> None:
    ensure_form_data()
    for field in fields:
        wk = widget_key(field)
        if wk in st.session_state:
            st.session_state.form_data[field] = st.session_state[wk]


def sync_step_fields(step: int) -> None:
    sync_fields(STEP_FIELDS.get(step, []))


def clear_widget_cache() -> None:
    for key in list(st.session_state.keys()):
        if str(key).startswith("w_"):
            del st.session_state[key]


def is_rtl(lang: str) -> bool:
    return lang == "ar"


def inject_base_css(lang: str) -> None:
    rtl = is_rtl(lang)
    direction = "rtl" if rtl else "ltr"
    align = "right" if rtl else "left"
    st.markdown(
        f"""
        <style>
            html, body, [class*="css"] {{ direction: {direction}; text-align: {align}; }}
            .stButton button, .stDownloadButton button {{ border-radius: 8px; }}
            .small-note {{ font-size: 0.92rem; opacity: 0.92; }}
            .code-box {{ padding: 0.6rem 0.8rem; border: 1px solid #ddd; border-radius: 8px; }}
        </style>
        """,
        unsafe_allow_html=True,
    )


def get_text(lang: str) -> Dict:
    return TRANSLATIONS.get(lang, TRANSLATIONS["en"])


def init_state() -> None:
    ensure_form_data()
    if "lang" not in st.session_state:
        st.session_state.lang = "en"
    if "lang_selector" not in st.session_state:
        st.session_state.lang_selector = st.session_state.lang
    if "current_step" not in st.session_state:
        st.session_state.current_step = 1
    if "draft_token" not in st.session_state:
        st.session_state.draft_token = ""
    if "last_submit_payload" not in st.session_state:
        st.session_state.last_submit_payload = None
    if "github_message" not in st.session_state:
        st.session_state.github_message = ""
    if "loaded_from_query" not in st.session_state:
        st.session_state.loaded_from_query = False


def reset_form() -> None:
    lang = st.session_state.lang
    st.session_state.form_data = dict(FIELD_DEFAULTS)
    clear_widget_cache()
    st.session_state.current_step = 1
    st.session_state.draft_token = ""
    st.session_state.last_submit_payload = None
    st.session_state.github_message = ""
    st.session_state.lang = lang
    st.session_state.lang_selector = lang
    st.query_params.clear()
    st.rerun()


def valid_email(value: str) -> bool:
    return bool(re.fullmatch(r"[^@\s]+@[^@\s]+\.[^@\s]+", value.strip()))


def github_settings() -> Dict[str, str]:
    owner = DEFAULT_GITHUB_OWNER
    repo = DEFAULT_GITHUB_REPO
    token = ""
    branch = DEFAULT_GITHUB_BRANCH
    try:
        gh = st.secrets.get("github", {})
        owner = gh.get("owner", "")
        repo = gh.get("repo", "")
        token = gh.get("token", "")
        branch = gh.get("branch", "main")
    except Exception:
        pass
    owner = owner or DEFAULT_GITHUB_OWNER
    repo = repo or DEFAULT_GITHUB_REPO
    token = token or os.getenv("GITHUB_TOKEN", "")
    branch = branch or DEFAULT_GITHUB_BRANCH
    return {"owner": owner, "repo": repo, "token": token, "branch": branch}


def github_ready() -> bool:
    cfg = github_settings()
    return bool(cfg["owner"] and cfg["repo"] and cfg["token"])


def github_headers() -> Dict[str, str]:
    cfg = github_settings()
    headers = {"Accept": "application/vnd.github+json"}
    if cfg["token"]:
        headers["Authorization"] = f"Bearer {cfg['token']}"
    return headers


def github_get_file(path: str) -> Dict | None:
    cfg = github_settings()
    if not cfg["owner"] or not cfg["repo"]:
        return None
    url = f"https://api.github.com/repos/{cfg['owner']}/{cfg['repo']}/contents/{path}"
    r = requests.get(url, headers=github_headers(), params={"ref": cfg["branch"]}, timeout=30)
    if r.status_code == 404:
        return None
    r.raise_for_status()
    return r.json()


def github_put_json(path: str, payload: Dict, message: str) -> Tuple[bool, str]:
    cfg = github_settings()
    if not github_ready():
        return False, "GitHub is not configured."
    url = f"https://api.github.com/repos/{cfg['owner']}/{cfg['repo']}/contents/{path}"
    existing = github_get_file(path)
    body = {
        "message": message,
        "content": base64.b64encode(json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")).decode("utf-8"),
        "branch": cfg["branch"],
    }
    if existing and isinstance(existing, dict) and existing.get("sha"):
        body["sha"] = existing["sha"]
    r = requests.put(url, headers=github_headers(), json=body, timeout=30)
    if 200 <= r.status_code < 300:
        return True, "OK"
    try:
        detail = r.json()
    except Exception:
        detail = {"message": r.text}
    return False, detail.get("message", "GitHub write failed")


def load_draft_from_github(token: str) -> Tuple[bool, str, Dict | None]:
    path = f"{RESPONSE_PATH_ROOT}/drafts/{token}.json"
    item = github_get_file(path)
    if not item:
        return False, "missing", None
    try:
        content = base64.b64decode(item["content"]).decode("utf-8")
        payload = json.loads(content)
    except Exception:
        return False, "decode_error", None
    expires_at = payload.get("expires_at")
    if expires_at:
        expiry = datetime.fromisoformat(expires_at.replace("Z", "+00:00"))
        if datetime.now(timezone.utc) > expiry:
            return False, "expired", payload
    return True, "ok", payload


def build_payload(status: str) -> Dict:
    ensure_form_data()
    now = datetime.now(timezone.utc)
    fd = st.session_state.form_data
    data = {
        "app_version": APP_VERSION,
        "status": status,
        "saved_at": now.isoformat().replace("+00:00", "Z"),
        "language": st.session_state.lang,
        "institution_acronym": (fd.get("institution_acronym") or "").strip(),
        "institution_type": fd.get("institution_type"),
        "country_or_rec": (fd.get("country_or_rec_other") or "").strip() if fd.get("country_or_rec") == "Other" else fd.get("country_or_rec"),
        "respondent_title": (fd.get("respondent_title_other") or "").strip() if fd.get("respondent_title") == "other" else fd.get("respondent_title"),
        "email": (fd.get("email") or "").strip(),
        "consolidated_position": "",
        "overall_validation": fd.get("overall_validation"),
        "overall_validation_why": (fd.get("overall_validation_why") or "").strip(),
        "operational_usability": fd.get("operational_usability"),
        "operational_usability_why": (fd.get("operational_usability_why") or "").strip(),
        "strategic_comments": (fd.get("strategic_comments") or "").strip(),
        "domain_comments": (fd.get("domain_comments") or "").strip(),
        "priorities_to_strengthen": [],
        "top_3_revisions": (fd.get("top_3_revisions") or "").strip(),
        "final_institutional_position": fd.get("final_institutional_position"),
        "draft_token": st.session_state.draft_token,
        "current_step": st.session_state.current_step,
    }
    for row in STRATEGIC_ROWS + DOMAIN_ROWS:
        data[row] = fd.get(row)
        data[f"{row}_why"] = (fd.get(f"{row}_why") or "").strip()
    return data


def payload_to_csv_bytes(payload: Dict) -> bytes:
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=list(payload.keys()))
    writer.writeheader()
    writer.writerow(payload)
    return output.getvalue().encode("utf-8-sig")


def save_draft() -> Tuple[bool, str, Dict]:
    if not st.session_state.draft_token:
        st.session_state.draft_token = secrets.token_hex(4).upper()
    payload = build_payload("draft")
    payload["draft_token"] = st.session_state.draft_token
    payload["expires_at"] = (datetime.now(timezone.utc) + timedelta(hours=48)).isoformat().replace("+00:00", "Z")
    if github_ready():
        ok, msg = github_put_json(
            f"{RESPONSE_PATH_ROOT}/drafts/{st.session_state.draft_token}.json",
            payload,
            f"Save validation draft {st.session_state.draft_token}",
        )
        return ok, msg, payload
    return False, "GitHub is not configured.", payload


def submit_final() -> Tuple[bool, str, Dict]:
    payload = build_payload("submitted")
    timestamp = datetime.now(timezone.utc)
    sub_id = f"SUB-{timestamp.strftime('%Y%m%dT%H%M%SZ')}-{secrets.token_hex(3).upper()}"
    payload["submission_id"] = sub_id
    payload["submitted_at"] = timestamp.isoformat().replace("+00:00", "Z")
    path = f"{RESPONSE_PATH_ROOT}/submissions/{timestamp:%Y/%m/%d}/{sub_id}.json"
    if github_ready():
        ok, msg = github_put_json(path, payload, f"Add validation submission {sub_id}")
        return ok, msg, payload
    return False, "GitHub is not configured.", payload


def apply_payload_to_state(payload: Dict) -> None:
    ensure_form_data()
    for key in FIELD_DEFAULTS.keys():
        if key in payload:
            st.session_state.form_data[key] = payload[key]
    for row in STRATEGIC_ROWS + DOMAIN_ROWS:
        if row in payload:
            st.session_state.form_data[row] = payload[row]
        why_key = f"{row}_why"
        if why_key in payload:
            st.session_state.form_data[why_key] = payload[why_key]
    clear_widget_cache()
    st.session_state.lang = payload.get("language", st.session_state.lang)
    st.session_state.lang_selector = st.session_state.lang
    st.session_state.current_step = int(payload.get("current_step", 1))
    st.session_state.draft_token = payload.get("draft_token", st.session_state.draft_token)


def validate_step(step: int, txt: Dict) -> List[str]:
    errors: List[str] = []
    q = txt["questions"]
    if step == 1:
        if not (get_value("institution_acronym") or "").strip():
            errors.append(q["institution_acronym"])
        if not get_value("institution_type"):
            errors.append(q["institution_type"])
        if not get_value("country_or_rec"):
            errors.append(q["country_or_rec"])
        if get_value("country_or_rec") == "Other" and not (get_value("country_or_rec_other") or "").strip():
            errors.append(f"{q['country_or_rec']} - {txt['other_specify']}")
        if not get_value("respondent_title"):
            errors.append(q["respondent_title"])
        if get_value("respondent_title") == "other" and not (get_value("respondent_title_other") or "").strip():
            errors.append(f"{q['respondent_title']} - {txt['other_specify']}")
        if not (get_value("email") or "").strip() or not valid_email((get_value("email") or "")):
            errors.append(q["email"])
    elif step == 2:
        if not get_value("overall_validation"):
            errors.append(q["overall_validation"])
        if get_value("overall_validation") in {"go_with_reservations", "no_go"} and not (get_value("overall_validation_why") or "").strip():
            errors.append(f"{q['overall_validation']} - {txt['overall_why']}")
        if not get_value("operational_usability"):
            errors.append(q["operational_usability"])
        if get_value("operational_usability") in {"mostly_no", "no"} and not (get_value("operational_usability_why") or "").strip():
            errors.append(f"{q['operational_usability']} - {txt['overall_why']}")
    elif step == 3:
        for row in STRATEGIC_ROWS:
            if not get_value(row):
                errors.append(txt["strategic_rows"][row])
            if get_value(row) in {"go_with_reservations", "no_go"} and not (get_value(f"{row}_why") or "").strip():
                errors.append(f"{txt['strategic_rows'][row]} - {txt['overall_why']}")
    elif step == 4:
        for row in DOMAIN_ROWS:
            if not get_value(row):
                errors.append(txt["domain_rows"][row])
            if get_value(row) in {"go_with_reservations", "no_go"} and not (get_value(f"{row}_why") or "").strip():
                errors.append(f"{txt['domain_rows'][row]} - {txt['overall_why']}")
    elif step == 5:
        if not get_value("final_institutional_position"):
            errors.append(q["final_position"])
    return errors


def choice_index(options: List[str], value: str | None) -> int | None:
    if value in options:
        return options.index(value)
    return None


def render_reference_links(txt: Dict) -> None:
    st.markdown(f"**{txt['ref_docs']}**")
    st.caption(txt["doc_links"])
    doc_cols = st.columns(2)
    doc_cols[0].link_button(txt["doc_en"], DEFAULT_DOC_URL_EN)
    doc_cols[1].link_button(txt["doc_fr"], DEFAULT_DOC_URL_FR)
    st.caption(txt["note_downloads"])
    note_cols = st.columns(4)
    note_keys = [("en", "note_en"), ("fr", "note_fr"), ("pt", "note_pt"), ("ar", "note_ar")]
    for idx, (lang_code, label_key) in enumerate(note_keys):
        note_url = DEFAULT_NOTE_URLS.get(lang_code, "")
        if note_url:
            note_cols[idx].link_button(txt[label_key], note_url)


def render_sidebar(txt: Dict) -> None:
    st.sidebar.markdown(f"### {txt['lang']}")
    selected_lang = st.sidebar.selectbox(
        txt["lang"],
        options=list(LANGUAGE_OPTIONS.keys()),
        index=list(LANGUAGE_OPTIONS.keys()).index(st.session_state.lang),
        format_func=lambda x: LANGUAGE_OPTIONS[x],
        key="lang_selector",
    )
    if selected_lang != st.session_state.lang:
        st.session_state.lang = selected_lang
        st.rerun()
    st.sidebar.progress(st.session_state.current_step / STEP_COUNT, text=f"{txt['step_label']} : {st.session_state.current_step}/{STEP_COUNT}")
    st.sidebar.caption(txt["sidebar_help"])
    if not github_ready():
        st.sidebar.info(txt["sidebar_repo_missing"])
    st.sidebar.markdown("---")
    if st.sidebar.button(txt["save_draft"], use_container_width=True):
        with st.spinner(txt["save_label"]):
            ok, msg, payload = save_draft()
        if ok:
            st.sidebar.success(txt["draft_saved"])
            st.sidebar.markdown(f"**{txt['draft_code_note']} :** `{st.session_state.draft_token}`")
            st.query_params["draft"] = st.session_state.draft_token
        else:
            st.sidebar.warning(msg)
        st.sidebar.download_button(
            txt["draft_download"],
            data=json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8"),
            file_name=f"validation_draft_{st.session_state.draft_token or 'draft'}.json",
            mime="application/json",
            use_container_width=True,
        )
    st.sidebar.text_input(
        txt["draft_code"],
        key="draft_code_input",
        placeholder=txt["draft_code_placeholder"],
    )
    if st.sidebar.button(txt["load_draft"], use_container_width=True):
        code = st.session_state.get("draft_code_input", "").strip()
        if code:
            ok, status, payload = load_draft_from_github(code)
            if ok and payload:
                apply_payload_to_state(payload)
                st.query_params["draft"] = code
                st.sidebar.success(txt["draft_loaded"])
                st.rerun()
            elif status == "expired":
                st.sidebar.error(txt["draft_expired"])
            else:
                st.sidebar.error(txt["draft_missing"])
    current_payload = build_payload("working_copy")
    st.sidebar.download_button(
        txt["response_download_json"],
        data=json.dumps(current_payload, ensure_ascii=False, indent=2).encode("utf-8"),
        file_name="validation_working_copy.json",
        mime="application/json",
        use_container_width=True,
    )
    st.sidebar.download_button(
        txt["response_download_csv"],
        data=payload_to_csv_bytes(current_payload),
        file_name="validation_working_copy.csv",
        mime="text/csv",
        use_container_width=True,
    )
    st.sidebar.markdown("---")
    if st.sidebar.button(txt["start_over"], use_container_width=True):
        reset_form()


def maybe_load_query_draft(txt: Dict) -> None:
    if st.session_state.loaded_from_query:
        return
    draft_code = st.query_params.get("draft")
    if draft_code:
        ok, status, payload = load_draft_from_github(str(draft_code))
        if ok and payload:
            apply_payload_to_state(payload)
            st.session_state.loaded_from_query = True
            st.info(txt["draft_loaded"])
        elif status == "expired":
            st.warning(txt["draft_expired"])
            st.session_state.loaded_from_query = True
        else:
            st.session_state.loaded_from_query = True


def render_choice_field(label: str, code_key: str, options_map: Dict[str, str], required_reason_values: set[str], txt: Dict) -> None:
    value_key = prime_widget(code_key)
    st.radio(
        label,
        options=list(options_map.keys()),
        index=choice_index(list(options_map.keys()), st.session_state[value_key]),
        format_func=lambda x: options_map[x],
        key=value_key,
        horizontal=False,
    )
    if st.session_state[value_key] in required_reason_values:
        why_key = prime_widget(f"{code_key}_why")
        st.text_area(
            txt["overall_why"],
            key=why_key,
            placeholder=txt["placeholders"]["why"],
        )


def render_step_1(txt: Dict) -> None:
    q = txt["questions"]
    st.subheader(txt["sections"][1])
    st.caption(txt["questions_required"])
    institution_acronym_key = prime_widget("institution_acronym")
    institution_type_key = prime_widget("institution_type")
    country_or_rec_key = prime_widget("country_or_rec")
    respondent_title_key = prime_widget("respondent_title")
    email_key = prime_widget("email")

    st.text_input(
        q["institution_acronym"],
        key=institution_acronym_key,
        placeholder=txt["placeholders"]["institution_acronym"],
    )
    st.selectbox(
        q["institution_type"],
        options=INSTITUTION_TYPES,
        index=choice_index(INSTITUTION_TYPES, st.session_state[institution_type_key]),
        placeholder="—",
        format_func=lambda x: txt["institution_types"][x],
        key=institution_type_key,
    )
    st.selectbox(
        q["country_or_rec"],
        options=COUNTRY_OR_REC_OPTIONS,
        index=choice_index(COUNTRY_OR_REC_OPTIONS, st.session_state[country_or_rec_key]),
        placeholder="—",
        format_func=lambda x: x,
        key=country_or_rec_key,
    )
    if st.session_state[country_or_rec_key] == "Other":
        st.text_input(txt["other_country"], key=prime_widget("country_or_rec_other"))
    st.selectbox(
        q["respondent_title"],
        options=RESPONDENT_TITLES,
        index=choice_index(RESPONDENT_TITLES, st.session_state[respondent_title_key]),
        placeholder="—",
        format_func=lambda x: txt["titles"][x],
        key=respondent_title_key,
    )
    if st.session_state[respondent_title_key] == "other":
        st.text_input(txt["other_specify"], key=prime_widget("respondent_title_other"))
    st.text_input(q["email"], key=email_key, placeholder=txt["placeholders"]["email"])


def render_step_2(txt: Dict) -> None:
    q = txt["questions"]
    st.subheader(txt["sections"][2])
    render_choice_field(q["overall_validation"], "overall_validation", txt["responses"], {"go_with_reservations", "no_go"}, txt)
    render_choice_field(q["operational_usability"], "operational_usability", txt["usability"], {"mostly_no", "no"}, txt)


def render_grid_section(rows: List[str], label: str, comments_key: str, options_map: Dict[str, str], txt: Dict, section_key: str) -> None:
    st.markdown(f"**{label}**")
    labels = txt[section_key]
    option_keys = list(options_map.keys())
    for row in rows:
        row_widget_key = prime_widget(row)
        st.markdown(f"**{labels[row]}**")
        st.radio(
            labels[row],
            options=option_keys,
            index=choice_index(option_keys, st.session_state[row_widget_key]),
            format_func=lambda x: options_map[x],
            key=row_widget_key,
            label_visibility="collapsed",
            horizontal=True,
        )
        if st.session_state[row_widget_key] in {"go_with_reservations", "no_go"}:
            st.text_area(
                txt["overall_why"],
                key=prime_widget(f"{row}_why"),
                placeholder=txt["placeholders"]["why"],
            )
        st.markdown("---")
    st.text_area(
        txt["optional_summary"],
        key=prime_widget(comments_key),
        placeholder=txt["placeholders"]["summary"],
    )


def render_step_3(txt: Dict) -> None:
    q = txt["questions"]
    st.subheader(txt["sections"][3])
    render_grid_section(STRATEGIC_ROWS, q["strategic_grid"], "strategic_comments", txt["responses"], txt, "strategic_rows")


def render_step_4(txt: Dict) -> None:
    q = txt["questions"]
    st.subheader(txt["sections"][4])
    render_grid_section(DOMAIN_ROWS, q["domain_grid"], "domain_comments", txt["responses"], txt, "domain_rows")
    st.text_area(q["top_3_revisions"], key=prime_widget("top_3_revisions"), placeholder=txt["placeholders"]["revisions"])


def render_step_5(txt: Dict) -> None:
    q = txt["questions"]
    st.subheader(txt["sections"][5])
    final_position_key = prime_widget("final_institutional_position")
    final_options = list(txt["final_positions"].keys())
    st.radio(
        q["final_position"],
        options=final_options,
        index=choice_index(final_options, st.session_state[final_position_key]),
        format_func=lambda x: txt["final_positions"][x],
        key=final_position_key,
        horizontal=False,
    )


def render_step_form(txt: Dict) -> None:
    step = st.session_state.current_step
    with st.form(key=f"step_form_{step}", clear_on_submit=False):
        if step == 1:
            render_step_1(txt)
        elif step == 2:
            render_step_2(txt)
        elif step == 3:
            render_step_3(txt)
        elif step == 4:
            render_step_4(txt)
        elif step == 5:
            render_step_5(txt)

        cols = st.columns([1, 1, 2])
        back_clicked = False
        if step > 1:
            back_clicked = cols[0].form_submit_button(txt["back"], use_container_width=True)
        if step < STEP_COUNT:
            continue_clicked = cols[1].form_submit_button(txt["continue"], use_container_width=True)
            if continue_clicked:
                sync_step_fields(step)
                errors = validate_step(step, txt)
                if errors:
                    st.error(txt["validation_title"])
                    for err in errors:
                        st.write(f"- {err}")
                else:
                    clear_widget_cache()
                    st.session_state.current_step += 1
                    st.rerun()
        else:
            submit_clicked = cols[1].form_submit_button(txt["submit"], use_container_width=True)
            if submit_clicked:
                sync_step_fields(step)
                all_errors: List[str] = []
                for step_num in range(1, STEP_COUNT + 1):
                    all_errors.extend(validate_step(step_num, txt))
                if all_errors:
                    st.error(txt["validation_title"])
                    for err in all_errors:
                        st.write(f"- {err}")
                else:
                    with st.spinner(txt["save_label"]):
                        ok, msg, payload = submit_final()
                    st.session_state.last_submit_payload = payload
                    if ok:
                        st.success(txt["submit_success"])
                        st.session_state.github_message = msg
                    else:
                        st.warning(txt["submit_warning"])
                        st.session_state.github_message = msg

        if back_clicked:
            sync_step_fields(step)
            clear_widget_cache()
            st.session_state.current_step -= 1
            st.rerun()


def render_submission_downloads(txt: Dict) -> None:
    payload = st.session_state.last_submit_payload
    if not payload:
        return
    st.download_button(
        txt["response_download_json"],
        data=json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8"),
        file_name=f"{payload.get('submission_id', 'validation_response')}.json",
        mime="application/json",
        use_container_width=True,
    )
    st.download_button(
        txt["response_download_csv"],
        data=payload_to_csv_bytes(payload),
        file_name=f"{payload.get('submission_id', 'validation_response')}.csv",
        mime="text/csv",
        use_container_width=True,
    )


def main() -> None:
    st.set_page_config(page_title="Strategic validation form", page_icon="📝", layout="wide")
    init_state()
    txt = get_text(st.session_state.lang)
    inject_base_css(st.session_state.lang)
    render_sidebar(txt)
    txt = get_text(st.session_state.lang)
    inject_base_css(st.session_state.lang)
    maybe_load_query_draft(txt)

    st.title(txt["title"])
    st.caption(txt["subtitle"])
    st.write(txt["intro"])
    st.write(txt["intro2"])
    st.write(txt["intro3"])
    st.write(txt["intro4"])
    st.markdown(f"**{txt['scale_title']}**")
    for item in txt["scale"]:
        st.write(f"- {item}")
    st.caption(txt["estimated"])
    render_reference_links(txt)
    st.progress(st.session_state.current_step / STEP_COUNT, text=f"{txt['step_label']} : {st.session_state.current_step}/{STEP_COUNT}")
    st.markdown("---")

    render_step_form(txt)
    if st.session_state.github_message:
        st.caption(st.session_state.github_message)
    render_submission_downloads(txt)
    st.markdown("---")
    st.caption(f"Streamlit • GitHub • v{APP_VERSION}")


if __name__ == "__main__":
    main()
