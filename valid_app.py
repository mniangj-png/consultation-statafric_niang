import base64
import csv
import io
import json
import os
import re
import uuid
from datetime import datetime
from typing import Dict, List, Optional

import requests
import streamlit as st

APP_VERSION = "2026-04-12"

# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------
DEFAULT_NOTE_URL = os.getenv("NOTE_URL", "")
DEFAULT_FULL_DOC_URL = os.getenv("FULL_DOC_URL", "")
DEFAULT_INTRO_IMAGE_URL = os.getenv("INTRO_IMAGE_URL", "")

LANGUAGE_OPTIONS = {
    "en": "English",
    "fr": "Français",
    "pt": "Português",
    "ar": "العربية",
}

RESPONSE_CODES = ["go", "go_with_reservations", "no_go", "no_opinion"]
USABILITY_CODES = ["yes", "mostly_yes", "mostly_no", "no", "no_opinion"]
FINAL_POSITION_CODES = [
    "yes",
    "yes_limited_adjustments",
    "no_substantial_revision",
    "discuss_in_workshop",
    "no_opinion",
]
INSTITUTION_TYPES = ["nso", "rec"]

TRANSLATIONS: Dict[str, Dict] = {
    "en": {
        "app_title": "Strategic validation of the draft document on priority socio-economic statistics in Africa",
        "app_subtitle": "Multilingual institutional questionnaire built from the decision-oriented summary note.",
        "language_label": "Language",
        "intro": (
            "Please complete this form on behalf of your institution. It is designed to collect a structured "
            "institutional position on the main strategic choices of the draft document on priority socio-economic "
            "statistics in Africa, with due consideration of the gender dimension."
        ),
        "intro_2": (
            "This form complements the decision-oriented summary note and the full draft document. It does not replace "
            "a detailed technical review, but it helps secure institutional Go/No-Go decisions on the main methodological, "
            "thematic, and operational choices."
        ),
        "intro_3": "Please respond after internal consultation whenever possible.",
        "response_scale_title": "Response scale used",
        "response_scale": [
            "Go: overall agreement",
            "Go with reservations: agreement subject to limited adjustments",
            "No-Go: major revision requested",
            "No opinion: no position at this stage",
        ],
        "estimated_time": "Estimated completion time: 10 to 12 minutes.",
        "links_title": "Reference documents",
        "note_link": "Decision-oriented summary note",
        "doc_link": "Full draft document",
        "submit_success": "Thank you. Your response has been recorded successfully.",
        "submit_warning": "Your response was validated locally, but online saving failed. Please download your response file below and share it manually.",
        "download_json": "Download response (JSON)",
        "download_csv": "Download response (CSV)",
        "reset_form": "Start a new response",
        "saving": "Saving your response...",
        "github_ok": "Your response was saved to the configured GitHub repository.",
        "github_missing": "GitHub saving is not configured. The app will still let respondents download their response locally.",
        "github_error": "GitHub saving failed",
        "validation_error_title": "Please complete the required fields before submitting.",
        "footer": "Built with Streamlit for multilingual strategic validation.",
        "sections": {
            "s1": "Section 1. Identification of the respondent",
            "s2": "Section 2. Overall validation",
            "s3": "Section 3. Validation of strategic choices",
            "s4": "Section 4. Validation of thematic domains",
            "s5": "Section 5. Final institutional position",
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
            "strategic_comments": (
                "9. If you selected ‘Go with reservations’ or ‘No-Go’ for one or more elements above, "
                "please specify which ones and why"
            ),
            "domain_grid": "10. Please assess the 12 proposed thematic domains",
            "domain_comments": (
                "11. If you selected ‘Go with reservations’ or ‘No-Go’ for one or more domains, "
                "please specify which ones and why"
            ),
            "top_3_revisions": "12. What are the three most important revisions needed before final validation?",
            "final_position": (
                "13. Is your institution broadly in favor of finalizing the document after consideration of comments received?"
            ),
        },
        "institution_type_options": {
            "nso": "National Statistical Office",
            "rec": "Regional Economic Community",
        },
        "response_options": {
            "go": "Go",
            "go_with_reservations": "Go with reservations",
            "no_go": "No-Go",
            "no_opinion": "No opinion",
        },
        "usability_options": {
            "yes": "Yes",
            "mostly_yes": "Mostly yes",
            "mostly_no": "Mostly no",
            "no": "No",
            "no_opinion": "No opinion",
        },
        "final_position_options": {
            "yes": "Yes",
            "yes_limited_adjustments": "Yes, subject to limited adjustments",
            "no_substantial_revision": "No, more substantial revision is needed",
            "discuss_in_workshop": "To be discussed in a workshop",
            "no_opinion": "No opinion",
        },
        "strategic_rows": {
            "strategic_prioritization_criteria": "Prioritization criteria used",
            "strategic_scoring_logic": "Scoring logic",
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
        "form_buttons": {
            "submit": "Submit response",
        },
        "placeholders": {
            "institution_acronym": "e.g. ANSD / COMESA",
            "country_or_rec": "e.g. Senegal / COMESA",
            "respondent_title": "e.g. Director General / Director of Statistics",
            "strategic_comments": "Brief justification for reservations or rejection",
            "domain_comments": "Brief justification for reservations or rejection",
            "top_3_revisions": "List the 3 most important revisions before final validation",
        },
    },
    "fr": {
        "app_title": "Validation stratégique du projet de document sur les statistiques socio-économiques prioritaires en Afrique",
        "app_subtitle": "Questionnaire institutionnel multilingue construit à partir de la note de synthèse décisionnelle.",
        "language_label": "Langue",
        "intro": (
            "Merci de renseigner ce formulaire au nom de votre institution. Il vise à recueillir une position structurée "
            "sur les principaux choix stratégiques du projet de document relatif à l’identification des statistiques "
            "socio-économiques prioritaires en Afrique, avec prise en compte de la dimension genre."
        ),
        "intro_2": (
            "Ce formulaire complète la note de synthèse décisionnelle et le document complet. Il ne remplace pas la "
            "relecture détaillée, mais permet de sécuriser rapidement les arbitrages institutionnels sur les principaux "
            "choix méthodologiques, thématiques et opérationnels."
        ),
        "intro_3": "Merci de répondre, dans la mesure du possible, après concertation interne.",
        "response_scale_title": "Échelle de réponse utilisée",
        "response_scale": [
            "Validé : accord global",
            "Validé sous réserve : accord sous ajustements limités",
            "Non-validé : révision importante demandée",
            "Sans avis : pas de position à ce stade",
        ],
        "estimated_time": "Temps estimé de réponse : 10 à 12 minutes.",
        "links_title": "Documents de référence",
        "note_link": "Note de synthèse décisionnelle",
        "doc_link": "Document complet",
        "submit_success": "Merci. Votre réponse a été enregistrée avec succès.",
        "submit_warning": "Votre réponse a été validée localement, mais l’enregistrement en ligne a échoué. Merci de télécharger le fichier de réponse ci-dessous et de le transmettre manuellement.",
        "download_json": "Télécharger la réponse (JSON)",
        "download_csv": "Télécharger la réponse (CSV)",
        "reset_form": "Commencer une nouvelle réponse",
        "saving": "Enregistrement de votre réponse...",
        "github_ok": "Votre réponse a été enregistrée dans le dépôt GitHub configuré.",
        "github_missing": "L’enregistrement GitHub n’est pas configuré. L’application permet toutefois aux répondants de télécharger leur réponse localement.",
        "github_error": "Échec de l’enregistrement GitHub",
        "validation_error_title": "Merci de renseigner les champs obligatoires avant soumission.",
        "footer": "Application Streamlit pour la validation stratégique multilingue.",
        "sections": {
            "s1": "Section 1. Identification du répondant",
            "s2": "Section 2. Validation générale",
            "s3": "Section 3. Validation des choix stratégiques",
            "s4": "Section 4. Validation des domaines thématiques",
            "s5": "Section 5. Position finale de l’institution",
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
            "strategic_comments": (
                "9. Si vous avez indiqué ‘Validé sous réserve’ ou ‘Non-validé’ pour un ou plusieurs éléments ci-dessus, "
                "merci de préciser lesquels et pourquoi"
            ),
            "domain_grid": "10. Veuillez apprécier les 12 domaines thématiques proposés",
            "domain_comments": (
                "11. Si vous avez indiqué ‘Validé sous réserve’ ou ‘Non-validé’ pour un ou plusieurs domaines, "
                "merci de préciser lesquels et pourquoi"
            ),
            "top_3_revisions": "12. Quelles sont les trois révisions les plus importantes à apporter avant validation finale ?",
            "final_position": "13. Votre institution est-elle globalement favorable à la finalisation du document après prise en compte des observations reçues ?",
        },
        "institution_type_options": {
            "nso": "Institut national de statistique (INS)",
            "rec": "Communauté économique régionale (CER)",
        },
        "response_options": {
            "go": "Validé",
            "go_with_reservations": "Validé sous réserve",
            "no_go": "Non-validé",
            "no_opinion": "Sans avis",
        },
        "usability_options": {
            "yes": "Oui",
            "mostly_yes": "Plutôt oui",
            "mostly_no": "Plutôt non",
            "no": "Non",
            "no_opinion": "Sans avis",
        },
        "final_position_options": {
            "yes": "Oui",
            "yes_limited_adjustments": "Oui, sous réserve d’ajustements limités",
            "no_substantial_revision": "Non, une révision plus substantielle est nécessaire",
            "discuss_in_workshop": "À discuter en atelier",
            "no_opinion": "Sans avis",
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
        "form_buttons": {
            "submit": "Soumettre la réponse",
        },
        "placeholders": {
            "institution_acronym": "ex. ANSD / COMESA",
            "country_or_rec": "ex. Sénégal / COMESA",
            "respondent_title": "ex. Directeur général / Directeur des statistiques",
            "strategic_comments": "Justification brève des réserves ou du rejet",
            "domain_comments": "Justification brève des réserves ou du rejet",
            "top_3_revisions": "Indiquez les 3 révisions les plus importantes avant validation finale",
        },
    },
    "pt": {
        "app_title": "Validação estratégica do projeto de documento sobre estatísticas socioeconómicas prioritárias em África",
        "app_subtitle": "Questionário institucional multilingue construído a partir da nota de síntese decisional.",
        "language_label": "Idioma",
        "intro": (
            "Por favor, preencha este formulário em nome da sua instituição. O objetivo é recolher uma posição "
            "estruturada sobre as principais escolhas estratégicas do projeto de documento relativo à identificação "
            "das estatísticas socioeconómicas prioritárias em África, com consideração da dimensão de género."
        ),
        "intro_2": (
            "Este formulário complementa a nota de síntese decisional e o documento completo. Não substitui a revisão "
            "técnica detalhada, mas ajuda a assegurar rapidamente as decisões institucionais de Go/No-Go sobre as "
            "principais escolhas metodológicas, temáticas e operacionais."
        ),
        "intro_3": "Responda, sempre que possível, após consulta interna.",
        "response_scale_title": "Escala de resposta utilizada",
        "response_scale": [
            "Validado: acordo global",
            "Validado com reservas: acordo sujeito a ajustes limitados",
            "Não validado: revisão importante solicitada",
            "Sem opinião: sem posição nesta fase",
        ],
        "estimated_time": "Tempo estimado de resposta: 10 a 12 minutos.",
        "links_title": "Documentos de referência",
        "note_link": "Nota de síntese decisional",
        "doc_link": "Documento completo",
        "submit_success": "Obrigado. A sua resposta foi registada com sucesso.",
        "submit_warning": "A sua resposta foi validada localmente, mas o registo online falhou. Por favor, descarregue o ficheiro de resposta abaixo e partilhe-o manualmente.",
        "download_json": "Descarregar resposta (JSON)",
        "download_csv": "Descarregar resposta (CSV)",
        "reset_form": "Iniciar uma nova resposta",
        "saving": "A guardar a sua resposta...",
        "github_ok": "A sua resposta foi guardada no repositório GitHub configurado.",
        "github_missing": "O armazenamento no GitHub não está configurado. A aplicação continuará a permitir o descarregamento local da resposta.",
        "github_error": "Falha ao guardar no GitHub",
        "validation_error_title": "Preencha os campos obrigatórios antes de submeter.",
        "footer": "Aplicação Streamlit para validação estratégica multilingue.",
        "sections": {
            "s1": "Secção 1. Identificação do respondente",
            "s2": "Secção 2. Validação geral",
            "s3": "Secção 3. Validação das escolhas estratégicas",
            "s4": "Secção 4. Validação dos domínios temáticos",
            "s5": "Secção 5. Posição final da instituição",
        },
        "questions": {
            "institution_acronym": "1. Sigla da instituição",
            "institution_type": "2. Tipo de instituição",
            "country_or_rec": "3. País ou CER representada",
            "respondent_title": "4. Função do principal respondente",
            "email": "5. Email",
            "overall_validation": "6. Validação global do documento",
            "operational_usability": "7. O documento parece suficientemente operacional para ser usado pelos Estados-Membros e pelas CER?",
            "strategic_grid": "8. Avalie os seguintes elementos estratégicos",
            "strategic_comments": (
                "9. Se selecionou ‘Validado com reservas’ ou ‘Não validado’ para um ou mais elementos acima, "
                "indique quais e porquê"
            ),
            "domain_grid": "10. Avalie os 12 domínios temáticos propostos",
            "domain_comments": (
                "11. Se selecionou ‘Validado com reservas’ ou ‘Não validado’ para um ou mais domínios, "
                "indique quais e porquê"
            ),
            "top_3_revisions": "12. Quais são as três revisões mais importantes a introduzir antes da validação final?",
            "final_position": "13. A sua instituição é globalmente favorável à finalização do documento após consideração das observações recebidas?",
        },
        "institution_type_options": {
            "nso": "Instituto Nacional de Estatística",
            "rec": "Comunidade Económica Regional",
        },
        "response_options": {
            "go": "Validado",
            "go_with_reservations": "Validado com reservas",
            "no_go": "Não validado",
            "no_opinion": "Sem opinião",
        },
        "usability_options": {
            "yes": "Sim",
            "mostly_yes": "Mais sim do que não",
            "mostly_no": "Mais não do que sim",
            "no": "Não",
            "no_opinion": "Sem opinião",
        },
        "final_position_options": {
            "yes": "Sim",
            "yes_limited_adjustments": "Sim, sujeito a ajustes limitados",
            "no_substantial_revision": "Não, é necessária uma revisão mais substancial",
            "discuss_in_workshop": "A discutir em atelier",
            "no_opinion": "Sem opinião",
        },
        "strategic_rows": {
            "strategic_prioritization_criteria": "Critérios de priorização adotados",
            "strategic_scoring_logic": "Lógica de pontuação multicritério",
            "strategic_core_extensions": "Distinção núcleo / extensões",
            "strategic_gender_integration": "Integração transversal do género",
            "strategic_min_disaggregations": "Desagregações mínimas propostas",
            "strategic_data_sources": "Fontes de dados e dispositivos de produção",
            "strategic_governance_roles": "Governação e repartição dos papéis",
            "strategic_roadmap_update": "Roteiro de implementação e mecanismo de atualização",
        },
        "domain_rows": {
            "domain_d01": "D01 Crescimento económico, transformação estrutural e comércio",
            "domain_d02": "D02 Emprego, trabalho digno e proteção social",
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
        "form_buttons": {
            "submit": "Submeter resposta",
        },
        "placeholders": {
            "institution_acronym": "ex. INE / COMESA",
            "country_or_rec": "ex. Moçambique / COMESA",
            "respondent_title": "ex. Diretor-Geral / Diretor de Estatísticas",
            "strategic_comments": "Justificação breve das reservas ou da rejeição",
            "domain_comments": "Justificação breve das reservas ou da rejeição",
            "top_3_revisions": "Indique as 3 revisões mais importantes antes da validação final",
        },
    },
    "ar": {
        "app_title": "التحقق الاستراتيجي من مشروع الوثيقة بشأن الإحصاءات الاجتماعية والاقتصادية ذات الأولوية في أفريقيا",
        "app_subtitle": "استبيان مؤسسي متعدد اللغات مبني على مذكرة التلخيص القرارّية.",
        "language_label": "اللغة",
        "intro": (
            "يرجى تعبئة هذا النموذج باسم مؤسستكم. يهدف هذا النموذج إلى جمع موقف مؤسسي منظم بشأن الخيارات الاستراتيجية "
            "الرئيسية في مشروع الوثيقة المتعلقة بتحديد الإحصاءات الاجتماعية والاقتصادية ذات الأولوية في أفريقيا، مع مراعاة "
            "البعد المتعلق بالنوع الاجتماعي."
        ),
        "intro_2": (
            "يكمل هذا النموذج مذكرة التلخيص القرارّية والوثيقة الكاملة. وهو لا يحل محل المراجعة الفنية التفصيلية، "
            "لكنه يساعد على تأمين قرارات القبول أو الرفض المؤسسية بسرعة فيما يتعلق بأهم الخيارات المنهجية والموضوعية والتشغيلية."
        ),
        "intro_3": "يرجى الإجابة، قدر الإمكان، بعد التشاور الداخلي داخل المؤسسة.",
        "response_scale_title": "مقياس الاستجابة المستخدم",
        "response_scale": [
            "مقبول: موافقة عامة",
            "مقبول مع تحفظات: موافقة مشروطة بتعديلات محدودة",
            "غير مقبول: مطلوب تنقيح مهم",
            "لا رأي: لا يوجد موقف في هذه المرحلة",
        ],
        "estimated_time": "الوقت التقديري للإجابة: من 10 إلى 12 دقيقة.",
        "links_title": "الوثائق المرجعية",
        "note_link": "مذكرة التلخيص القرارّية",
        "doc_link": "الوثيقة الكاملة",
        "submit_success": "شكرًا لكم. تم تسجيل إجابتكم بنجاح.",
        "submit_warning": "تم التحقق من إجابتكم محليًا، لكن الحفظ عبر الإنترنت فشل. يُرجى تنزيل ملف الإجابة أدناه ومشاركته يدويًا.",
        "download_json": "تنزيل الإجابة (JSON)",
        "download_csv": "تنزيل الإجابة (CSV)",
        "reset_form": "بدء إجابة جديدة",
        "saving": "جارٍ حفظ إجابتكم...",
        "github_ok": "تم حفظ إجابتكم في مستودع GitHub المهيأ.",
        "github_missing": "لم يتم تهيئة الحفظ على GitHub. ومع ذلك سيظل بإمكان المجيب تنزيل إجابته محليًا.",
        "github_error": "فشل الحفظ على GitHub",
        "validation_error_title": "يرجى استكمال الحقول الإلزامية قبل الإرسال.",
        "footer": "تطبيق Streamlit للتحقق الاستراتيجي متعدد اللغات.",
        "sections": {
            "s1": "القسم 1. تحديد هوية المجيب",
            "s2": "القسم 2. التحقق العام",
            "s3": "القسم 3. التحقق من الخيارات الاستراتيجية",
            "s4": "القسم 4. التحقق من المجالات الموضوعية",
            "s5": "القسم 5. الموقف النهائي للمؤسسة",
        },
        "questions": {
            "institution_acronym": "1. اختصار المؤسسة",
            "institution_type": "2. نوع المؤسسة",
            "country_or_rec": "3. البلد أو التجمع الإقليمي الممثل",
            "respondent_title": "4. صفة المجيب الرئيسي",
            "email": "5. البريد الإلكتروني",
            "overall_validation": "6. التحقق العام من الوثيقة",
            "operational_usability": "7. هل تبدو الوثيقة عملية بما يكفي لاستخدامها من قبل الدول الأعضاء والتجمعات الاقتصادية الإقليمية؟",
            "strategic_grid": "8. يرجى تقييم العناصر الاستراتيجية التالية",
            "strategic_comments": (
                "9. إذا اخترتم ‘مقبول مع تحفظات’ أو ‘غير مقبول’ لعنصر واحد أو أكثر أعلاه، "
                "فيرجى تحديد العناصر المعنية وشرح السبب"
            ),
            "domain_grid": "10. يرجى تقييم المجالات الموضوعية الاثني عشر المقترحة",
            "domain_comments": (
                "11. إذا اخترتم ‘مقبول مع تحفظات’ أو ‘غير مقبول’ لمجال واحد أو أكثر، "
                "فيرجى تحديد المجالات المعنية وشرح السبب"
            ),
            "top_3_revisions": "12. ما هي أهم ثلاثة تعديلات ينبغي إدخالها قبل التحقق النهائي؟",
            "final_position": "13. هل مؤسستكم مؤيدة عمومًا لاستكمال الوثيقة بعد أخذ الملاحظات الواردة في الاعتبار؟",
        },
        "institution_type_options": {
            "nso": "المعهد الوطني للإحصاء",
            "rec": "تجمع اقتصادي إقليمي",
        },
        "response_options": {
            "go": "مقبول",
            "go_with_reservations": "مقبول مع تحفظات",
            "no_go": "غير مقبول",
            "no_opinion": "لا رأي",
        },
        "usability_options": {
            "yes": "نعم",
            "mostly_yes": "نعم إلى حد كبير",
            "mostly_no": "لا إلى حد ما",
            "no": "لا",
            "no_opinion": "لا رأي",
        },
        "final_position_options": {
            "yes": "نعم",
            "yes_limited_adjustments": "نعم، مع تعديلات محدودة",
            "no_substantial_revision": "لا، هناك حاجة إلى مراجعة أكثر جوهرية",
            "discuss_in_workshop": "يُناقش في ورشة عمل",
            "no_opinion": "لا رأي",
        },
        "strategic_rows": {
            "strategic_prioritization_criteria": "معايير تحديد الأولويات المعتمدة",
            "strategic_scoring_logic": "منطق التنقيط متعدد المعايير",
            "strategic_core_extensions": "التمييز بين النواة والامتدادات",
            "strategic_gender_integration": "الإدماج الأفقي للنوع الاجتماعي",
            "strategic_min_disaggregations": "التفصيلات الدنيا المقترحة",
            "strategic_data_sources": "مصادر البيانات وترتيبات الإنتاج",
            "strategic_governance_roles": "الحوكمة وتوزيع الأدوار",
            "strategic_roadmap_update": "خارطة طريق التنفيذ وآلية التحديث",
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
        "form_buttons": {
            "submit": "إرسال الإجابة",
        },
        "placeholders": {
            "institution_acronym": "مثال: ANSD / COMESA",
            "country_or_rec": "مثال: السنغال / COMESA",
            "respondent_title": "مثال: المدير العام / مدير الإحصاءات",
            "strategic_comments": "تعليل موجز للتحفظات أو الرفض",
            "domain_comments": "تعليل موجز للتحفظات أو الرفض",
            "top_3_revisions": "اذكر أهم 3 تعديلات قبل التحقق النهائي",
        },
    },
}


def t(lang: str, key_path: str):
    value = TRANSLATIONS[lang]
    for part in key_path.split("."):
        value = value[part]
    return value


st.set_page_config(page_title="Strategic validation questionnaire", layout="wide")


STRATEGIC_ROW_KEYS = [
    "strategic_prioritization_criteria",
    "strategic_scoring_logic",
    "strategic_core_extensions",
    "strategic_gender_integration",
    "strategic_min_disaggregations",
    "strategic_data_sources",
    "strategic_governance_roles",
    "strategic_roadmap_update",
]

DOMAIN_ROW_KEYS = [
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


def apply_direction(lang: str) -> None:
    rtl = lang == "ar"
    direction = "rtl" if rtl else "ltr"
    align = "right" if rtl else "left"
    st.markdown(
        f"""
        <style>
        html, body, [data-testid="stAppViewContainer"], [data-testid="stMarkdownContainer"] {{
            direction: {direction};
            text-align: {align};
        }}
        .block-container {{
            padding-top: 1.4rem;
            padding-bottom: 2rem;
        }}
        div[data-testid="stRadio"] > label {{
            font-weight: 600;
        }}
        .question-box {{
            border: 1px solid rgba(120, 120, 120, 0.25);
            border-radius: 12px;
            padding: 0.7rem 0.9rem 0.4rem 0.9rem;
            margin-bottom: 0.7rem;
            background: rgba(250,250,250,0.02);
        }}
        </style>
        """,
        unsafe_allow_html=True,
    )


def get_github_config() -> Dict[str, str]:
    secrets_github = st.secrets.get("github", {}) if hasattr(st, "secrets") else {}
    return {
        "owner": secrets_github.get("owner", os.getenv("GITHUB_REPO_OWNER", "")),
        "repo": secrets_github.get("repo", os.getenv("GITHUB_REPO_NAME", "")),
        "token": secrets_github.get("token", os.getenv("GITHUB_TOKEN", "")),
        "branch": secrets_github.get("branch", os.getenv("GITHUB_BRANCH", "main")),
        "folder": secrets_github.get("folder", os.getenv("GITHUB_SUBMISSIONS_FOLDER", "submissions")),
    }


def github_is_configured() -> bool:
    cfg = get_github_config()
    return bool(cfg["owner"] and cfg["repo"] and cfg["token"])


EMAIL_REGEX = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")


def option_dict(lang: str, group_key: str, codes: List[str]) -> Dict[str, str]:
    return {code: t(lang, f"{group_key}.{code}") for code in codes}


def safe_filename(text: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]+", "_", text.strip()) or "response"


def response_to_csv_bytes(payload: Dict) -> bytes:
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["field", "value"])
    flat_items = flatten_payload(payload)
    for key, value in flat_items.items():
        writer.writerow([key, json.dumps(value, ensure_ascii=False) if isinstance(value, (dict, list)) else value])
    return output.getvalue().encode("utf-8-sig")


def flatten_payload(payload: Dict) -> Dict[str, str]:
    flat: Dict[str, str] = {}

    def _flatten(prefix: str, value):
        if isinstance(value, dict):
            for sub_key, sub_value in value.items():
                next_prefix = f"{prefix}.{sub_key}" if prefix else sub_key
                _flatten(next_prefix, sub_value)
        else:
            flat[prefix] = value

    _flatten("", payload)
    return flat


def validate_submission(data: Dict) -> List[str]:
    missing = []
    required_scalar_keys = [
        "institution_acronym",
        "institution_type",
        "country_or_rec",
        "respondent_title",
        "email",
        "overall_validation",
        "operational_usability",
        "top_3_revisions",
        "final_institutional_position",
    ]

    for key in required_scalar_keys:
        if not data.get(key):
            missing.append(key)

    if data.get("email") and not EMAIL_REGEX.match(data["email"].strip()):
        missing.append("email_invalid")

    for key in STRATEGIC_ROW_KEYS + DOMAIN_ROW_KEYS:
        if not data.get(key):
            missing.append(key)

    return missing


def save_submission_to_github(payload: Dict) -> Dict[str, str]:
    cfg = get_github_config()
    if not github_is_configured():
        return {"ok": False, "reason": "not_configured"}

    submission_id = payload["meta"]["submission_id"]
    utc_now = datetime.utcnow()
    path = (
        f"{cfg['folder']}/{utc_now:%Y/%m/%d}/"
        f"{submission_id}.json"
    )
    api_url = f"https://api.github.com/repos/{cfg['owner']}/{cfg['repo']}/contents/{path}"
    content = base64.b64encode(
        json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
    ).decode("utf-8")

    body = {
        "message": f"Add validation submission {submission_id}",
        "content": content,
        "branch": cfg["branch"],
    }
    headers = {
        "Accept": "application/vnd.github+json",
        "Authorization": f"Bearer {cfg['token']}",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    response = requests.put(api_url, headers=headers, json=body, timeout=30)
    if response.status_code in (200, 201):
        return {"ok": True, "path": path}

    detail = ""
    try:
        detail = response.json().get("message", "")
    except Exception:
        detail = response.text[:300]
    return {
        "ok": False,
        "reason": f"http_{response.status_code}",
        "detail": detail,
        "path": path,
    }


def default_state() -> Dict:
    return {
        "language": "en",
        "submitted_payload": None,
        "last_save_status": None,
    }


def init_state() -> None:
    defaults = default_state()
    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value


def reset_form_state() -> None:
    keys_to_keep = {"language"}
    for key in list(st.session_state.keys()):
        if key not in keys_to_keep:
            del st.session_state[key]
    init_state()
    st.rerun()


def render_radio_question(
    label: str,
    options_map: Dict[str, str],
    key: str,
    horizontal: bool = True,
) -> Optional[str]:
    reverse = {v: k for k, v in options_map.items()}
    current_code = st.session_state.get(key)
    current_label = options_map.get(current_code) if current_code else None
    selected_label = st.radio(
        label,
        options=list(options_map.values()),
        index=None if current_label is None else list(options_map.values()).index(current_label),
        horizontal=horizontal,
        key=f"widget_{key}",
    )
    code = reverse.get(selected_label) if selected_label else None
    st.session_state[key] = code
    return code


def render_text_input(label: str, key: str, placeholder: str = "", input_type: str = "default") -> str:
    value = st.text_input(label, value=st.session_state.get(key, ""), placeholder=placeholder, key=f"widget_{key}", type=input_type)
    st.session_state[key] = value.strip()
    return st.session_state[key]


def render_text_area(label: str, key: str, placeholder: str = "", height: int = 120) -> str:
    value = st.text_area(label, value=st.session_state.get(key, ""), placeholder=placeholder, height=height, key=f"widget_{key}")
    st.session_state[key] = value.strip()
    return st.session_state[key]


def build_payload(lang: str) -> Dict:
    response_options = option_dict(lang, "response_options", RESPONSE_CODES)
    usability_options = option_dict(lang, "usability_options", USABILITY_CODES)
    institution_options = option_dict(lang, "institution_type_options", INSTITUTION_TYPES)
    final_options = option_dict(lang, "final_position_options", FINAL_POSITION_CODES)

    strategic_assessments = {}
    for row_key in STRATEGIC_ROW_KEYS:
        code = st.session_state.get(row_key)
        strategic_assessments[row_key] = {
            "code": code,
            "label": response_options.get(code, ""),
            "label_en": TRANSLATIONS["en"]["response_options"].get(code, ""),
        }

    domain_assessments = {}
    for row_key in DOMAIN_ROW_KEYS:
        code = st.session_state.get(row_key)
        domain_assessments[row_key] = {
            "code": code,
            "label": response_options.get(code, ""),
            "label_en": TRANSLATIONS["en"]["response_options"].get(code, ""),
        }

    submission_id = f"SUB-{datetime.utcnow():%Y%m%dT%H%M%SZ}-{uuid.uuid4().hex[:6].upper()}"
    payload = {
        "meta": {
            "submission_id": submission_id,
            "submitted_at_utc": datetime.utcnow().replace(microsecond=0).isoformat() + "Z",
            "app_version": APP_VERSION,
            "language": lang,
            "language_label": LANGUAGE_OPTIONS.get(lang, lang),
        },
        "institution_acronym": st.session_state.get("institution_acronym", ""),
        "institution_type": {
            "code": st.session_state.get("institution_type"),
            "label": institution_options.get(st.session_state.get("institution_type"), ""),
            "label_en": TRANSLATIONS["en"]["institution_type_options"].get(st.session_state.get("institution_type"), ""),
        },
        "country_or_rec": st.session_state.get("country_or_rec", ""),
        "respondent_title": st.session_state.get("respondent_title", ""),
        "email": st.session_state.get("email", ""),
        "overall_validation": {
            "code": st.session_state.get("overall_validation"),
            "label": response_options.get(st.session_state.get("overall_validation"), ""),
            "label_en": TRANSLATIONS["en"]["response_options"].get(st.session_state.get("overall_validation"), ""),
        },
        "operational_usability": {
            "code": st.session_state.get("operational_usability"),
            "label": usability_options.get(st.session_state.get("operational_usability"), ""),
            "label_en": TRANSLATIONS["en"]["usability_options"].get(st.session_state.get("operational_usability"), ""),
        },
        "strategic_assessments": strategic_assessments,
        "strategic_comments": st.session_state.get("strategic_comments", ""),
        "domain_assessments": domain_assessments,
        "domain_comments": st.session_state.get("domain_comments", ""),
        "top_3_revisions": st.session_state.get("top_3_revisions", ""),
        "final_institutional_position": {
            "code": st.session_state.get("final_institutional_position"),
            "label": final_options.get(st.session_state.get("final_institutional_position"), ""),
            "label_en": TRANSLATIONS["en"]["final_position_options"].get(st.session_state.get("final_institutional_position"), ""),
        },
        # Flat fields kept for easier export / consolidation
        "flat_export": {
            "institution_acronym": st.session_state.get("institution_acronym", ""),
            "institution_type": st.session_state.get("institution_type", ""),
            "country_or_rec": st.session_state.get("country_or_rec", ""),
            "respondent_title": st.session_state.get("respondent_title", ""),
            "email": st.session_state.get("email", ""),
            "overall_validation": st.session_state.get("overall_validation", ""),
            "operational_usability": st.session_state.get("operational_usability", ""),
            **{k: st.session_state.get(k, "") for k in STRATEGIC_ROW_KEYS},
            "strategic_comments": st.session_state.get("strategic_comments", ""),
            **{k: st.session_state.get(k, "") for k in DOMAIN_ROW_KEYS},
            "domain_comments": st.session_state.get("domain_comments", ""),
            "top_3_revisions": st.session_state.get("top_3_revisions", ""),
            "final_institutional_position": st.session_state.get("final_institutional_position", ""),
        },
    }
    return payload


def render_form(lang: str) -> None:
    response_options = option_dict(lang, "response_options", RESPONSE_CODES)
    usability_options = option_dict(lang, "usability_options", USABILITY_CODES)
    institution_options = option_dict(lang, "institution_type_options", INSTITUTION_TYPES)
    final_options = option_dict(lang, "final_position_options", FINAL_POSITION_CODES)

    st.header(t(lang, "sections.s1"))
    c1, c2 = st.columns(2)
    with c1:
        render_text_input(
            t(lang, "questions.institution_acronym"),
            "institution_acronym",
            placeholder=t(lang, "placeholders.institution_acronym"),
        )
        render_text_input(
            t(lang, "questions.country_or_rec"),
            "country_or_rec",
            placeholder=t(lang, "placeholders.country_or_rec"),
        )
        render_text_input(
            t(lang, "questions.email"),
            "email",
            input_type="default",
        )
    with c2:
        reverse_institution = {v: k for k, v in institution_options.items()}
        institution_values = list(institution_options.values())
        current_institution = institution_options.get(st.session_state.get("institution_type"))
        selected_institution = st.selectbox(
            t(lang, "questions.institution_type"),
            options=institution_values,
            index=None if current_institution is None else institution_values.index(current_institution),
            placeholder="...",
            key="widget_institution_type",
        )
        st.session_state["institution_type"] = reverse_institution.get(selected_institution) if selected_institution else None

        render_text_input(
            t(lang, "questions.respondent_title"),
            "respondent_title",
            placeholder=t(lang, "placeholders.respondent_title"),
        )

    st.divider()
    st.header(t(lang, "sections.s2"))
    render_radio_question(
        t(lang, "questions.overall_validation"),
        response_options,
        "overall_validation",
    )
    render_radio_question(
        t(lang, "questions.operational_usability"),
        usability_options,
        "operational_usability",
    )

    st.divider()
    st.header(t(lang, "sections.s3"))
    st.markdown(f"**{t(lang, 'questions.strategic_grid')}**")
    for row_key in STRATEGIC_ROW_KEYS:
        with st.container(border=True):
            render_radio_question(
                t(lang, f"strategic_rows.{row_key}"),
                response_options,
                row_key,
            )
    render_text_area(
        t(lang, "questions.strategic_comments"),
        "strategic_comments",
        placeholder=t(lang, "placeholders.strategic_comments"),
        height=140,
    )

    st.divider()
    st.header(t(lang, "sections.s4"))
    st.markdown(f"**{t(lang, 'questions.domain_grid')}**")
    for row_key in DOMAIN_ROW_KEYS:
        with st.container(border=True):
            render_radio_question(
                t(lang, f"domain_rows.{row_key}"),
                response_options,
                row_key,
            )
    render_text_area(
        t(lang, "questions.domain_comments"),
        "domain_comments",
        placeholder=t(lang, "placeholders.domain_comments"),
        height=140,
    )
    render_text_area(
        t(lang, "questions.top_3_revisions"),
        "top_3_revisions",
        placeholder=t(lang, "placeholders.top_3_revisions"),
        height=160,
    )

    st.divider()
    st.header(t(lang, "sections.s5"))
    render_radio_question(
        t(lang, "questions.final_position"),
        final_options,
        "final_institutional_position",
    )


def render_downloads(lang: str, payload: Dict) -> None:
    json_bytes = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
    csv_bytes = response_to_csv_bytes(payload)
    file_base = safe_filename(payload["meta"]["submission_id"])

    col1, col2, col3 = st.columns([1, 1, 1])
    with col1:
        st.download_button(
            label=t(lang, "download_json"),
            data=json_bytes,
            file_name=f"{file_base}.json",
            mime="application/json",
        )
    with col2:
        st.download_button(
            label=t(lang, "download_csv"),
            data=csv_bytes,
            file_name=f"{file_base}.csv",
            mime="text/csv",
        )
    with col3:
        if st.button(t(lang, "reset_form")):
            reset_form_state()


def main() -> None:
    init_state()

    lang = st.sidebar.selectbox(
        "Language / Langue / Idioma / اللغة",
        options=list(LANGUAGE_OPTIONS.keys()),
        format_func=lambda x: LANGUAGE_OPTIONS[x],
        index=list(LANGUAGE_OPTIONS.keys()).index(st.session_state.get("language", "en")),
    )
    st.session_state["language"] = lang
    apply_direction(lang)

    st.title(t(lang, "app_title"))
    st.caption(t(lang, "app_subtitle"))

    st.write(t(lang, "intro"))
    st.write(t(lang, "intro_2"))
    st.write(t(lang, "intro_3"))

    with st.expander(t(lang, "response_scale_title"), expanded=False):
        for item in t(lang, "response_scale"):
            st.write(f"- {item}")
        st.write(t(lang, "estimated_time"))

    with st.sidebar:
        st.subheader(t(lang, "links_title"))
        note_url = st.secrets.get("links", {}).get("note_url", DEFAULT_NOTE_URL) if hasattr(st, "secrets") else DEFAULT_NOTE_URL
        full_doc_url = st.secrets.get("links", {}).get("full_doc_url", DEFAULT_FULL_DOC_URL) if hasattr(st, "secrets") else DEFAULT_FULL_DOC_URL
        if note_url:
            st.markdown(f"- [{t(lang, 'note_link')}]({note_url})")
        if full_doc_url:
            st.markdown(f"- [{t(lang, 'doc_link')}]({full_doc_url})")

        st.divider()
        if github_is_configured():
            st.success(t(lang, "github_ok"))
        else:
            st.info(t(lang, "github_missing"))

    with st.form("strategic_validation_form", clear_on_submit=False):
        render_form(lang)
        submitted = st.form_submit_button(t(lang, "form_buttons.submit"))

    if submitted:
        data_for_validation = {
            "institution_acronym": st.session_state.get("institution_acronym"),
            "institution_type": st.session_state.get("institution_type"),
            "country_or_rec": st.session_state.get("country_or_rec"),
            "respondent_title": st.session_state.get("respondent_title"),
            "email": st.session_state.get("email"),
            "overall_validation": st.session_state.get("overall_validation"),
            "operational_usability": st.session_state.get("operational_usability"),
            "top_3_revisions": st.session_state.get("top_3_revisions"),
            "final_institutional_position": st.session_state.get("final_institutional_position"),
            **{k: st.session_state.get(k) for k in STRATEGIC_ROW_KEYS},
            **{k: st.session_state.get(k) for k in DOMAIN_ROW_KEYS},
        }
        errors = validate_submission(data_for_validation)
        if errors:
            st.error(t(lang, "validation_error_title"))
            for err in errors:
                st.write(f"- {err}")
        else:
            with st.spinner(t(lang, "saving")):
                payload = build_payload(lang)
                save_status = save_submission_to_github(payload)
                st.session_state["submitted_payload"] = payload
                st.session_state["last_save_status"] = save_status

    if st.session_state.get("submitted_payload"):
        payload = st.session_state["submitted_payload"]
        save_status = st.session_state.get("last_save_status") or {}
        if save_status.get("ok"):
            st.success(t(lang, "submit_success"))
        elif save_status.get("reason") == "not_configured":
            st.warning(t(lang, "submit_warning"))
        else:
            st.warning(t(lang, "submit_warning"))
            detail = save_status.get("detail", "")
            if detail:
                st.caption(f"{t(lang, 'github_error')}: {detail}")
        render_downloads(lang, payload)

    st.divider()
    st.caption(t(lang, "footer"))


if __name__ == "__main__":
    main()
