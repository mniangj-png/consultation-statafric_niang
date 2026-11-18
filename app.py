# HISWACA 2.1.0 — autonome ; aucun import diagnostic.
from __future__ import annotations

import json
SCHEMA=json.loads(r'''{
  "version": "2026-11-15.web-2",
  "app_version": "2.0.0",
  "reference_start": "2025-11-01",
  "reference_end": "2026-10-31",
  "collection_start": "2026-11-19T00:00:00Z",
  "collection_end": "2026-11-26T23:59:59Z",
  "languages": [
    "fr",
    "en",
    "ar",
    "pt",
    "es"
  ],
  "forms": {
    "ins": {
      "title": {
        "fr": "Instituts nationaux de la statistique",
        "en": "National statistical offices",
        "es": "Oficinas nacionales de estadística",
        "pt": "Institutos nacionais de estatística",
        "ar": "الأجهزة الوطنية للإحصاء"
      },
      "duration": {
        "fr": "15 à 20 minutes",
        "en": "15 to 20 minutes",
        "es": "De 15 a 20 minutos",
        "pt": "15 a 20 minutos",
        "ar": "من 15 إلى 20 دقيقة"
      },
      "sections": [
        {
          "id": "QI01",
          "title": {
            "fr": "Accord de participation et périmètre de la réponse",
            "en": "Participation agreement and scope of the response",
            "es": "Consentimiento y ámbito de la respuesta",
            "pt": "Consentimento e âmbito da resposta",
            "ar": "الموافقة على المشاركة ونطاق الإجابة"
          },
          "fields": [
            {
              "id": "QI01_Participation",
              "label": {
                "fr": "Acceptez-vous l’utilisation de votre contribution selon la notice ?",
                "en": "Do you agree to the use of your contribution as described in the notice?",
                "es": "¿Autoriza el uso de su contribución según el aviso de participación?",
                "pt": "Autoriza a utilização da sua contribuição conforme o aviso de participação?",
                "ar": "هل توافق على استخدام مساهمتك وفق إشعار المشاركة؟"
              },
              "type": "single",
              "options": [
                {
                  "code": 1,
                  "label": {
                    "fr": "J’accepte",
                    "en": "I agree",
                    "es": "Acepto",
                    "pt": "Aceito",
                    "ar": "أوافق"
                  }
                },
                {
                  "code": 0,
                  "label": {
                    "fr": "Je ne participe pas — fin du questionnaire",
                    "en": "I do not wish to participate — end of questionnaire",
                    "es": "No deseo participar — fin del cuestionario",
                    "pt": "Não desejo participar — fim do questionário",
                    "ar": "لا أرغب في المشاركة — نهاية الاستبيان"
                  }
                }
              ],
              "required": true,
              "source": "Rapport méthodologique du 15 novembre 2026, pp. 19–22, QI01"
            },
            {
              "id": "QI01_Pays",
              "label": {
                "fr": "Pays",
                "en": "Country",
                "es": "País",
                "pt": "País",
                "ar": "البلد"
              },
              "type": "text",
              "options": [],
              "max_chars": 500,
              "source": "Rapport méthodologique du 15 novembre 2026, pp. 19–22, QI01",
              "gate": {
                "field": "QI01_Participation",
                "op": "in",
                "values": [
                  1
                ]
              }
            },
            {
              "id": "QI01_INS",
              "label": {
                "fr": "Nom ou sigle de l’INS",
                "en": "Name or acronym of the NSO",
                "es": "Nombre o sigla de la ONE",
                "pt": "Nome ou sigla do INE",
                "ar": "اسم الجهاز الوطني للإحصاء أو اختصاره"
              },
              "type": "text",
              "options": [],
              "max_chars": 500,
              "source": "Rapport méthodologique du 15 novembre 2026, pp. 19–22, QI01",
              "gate": {
                "field": "QI01_Participation",
                "op": "in",
                "values": [
                  1
                ]
              }
            },
            {
              "id": "QI01_Statut",
              "label": {
                "fr": "Statut de la réponse",
                "en": "Response status",
                "es": "Carácter de la respuesta",
                "pt": "Estatuto da resposta",
                "ar": "صفة الإجابة"
              },
              "type": "single",
              "options": [
                {
                  "code": 1,
                  "label": {
                    "fr": "Réponse institutionnelle consolidée",
                    "en": "Consolidated institutional response",
                    "es": "Respuesta institucional consolidada",
                    "pt": "Resposta institucional consolidada",
                    "ar": "إجابة مؤسسية موحدة"
                  }
                },
                {
                  "code": 2,
                  "label": {
                    "fr": "Contribution d’un service, non consolidée au niveau de l’institution",
                    "en": "Service-level contribution, not consolidated at institution level",
                    "es": "Contribución de un servicio, no consolidada a nivel institucional",
                    "pt": "Contribuição de um serviço, não consolidada ao nível da instituição",
                    "ar": "مساهمة من إدارة، غير موحدة على مستوى المؤسسة"
                  }
                },
                {
                  "code": 3,
                  "label": {
                    "fr": "Contribution individuelle fondée sur ma pratique professionnelle",
                    "en": "Individual contribution based on my professional practice",
                    "es": "Contribución individual basada en mi práctica profesional",
                    "pt": "Contribuição individual baseada na minha prática profissional",
                    "ar": "مساهمة فردية تستند إلى ممارستي المهنية"
                  }
                }
              ],
              "source": "Rapport méthodologique du 15 novembre 2026, pp. 19–22, QI01",
              "gate": {
                "field": "QI01_Participation",
                "op": "in",
                "values": [
                  1
                ]
              }
            },
            {
              "id": "QI01_Fonction",
              "label": {
                "fr": "Fonction du répondant, sans nom personnel",
                "en": "Respondent’s function, without a personal name",
                "es": "Función de quien responde, sin nombre personal",
                "pt": "Função de quem responde, sem nome pessoal",
                "ar": "وظيفة المجيب، دون ذكر اسمه"
              },
              "type": "single",
              "options": [
                {
                  "code": 1,
                  "label": {
                    "fr": "Production statistique",
                    "en": "Statistical production",
                    "es": "Producción estadística",
                    "pt": "Produção estatística",
                    "ar": "الإنتاج الإحصائي"
                  }
                },
                {
                  "code": 2,
                  "label": {
                    "fr": "Communication ou diffusion",
                    "en": "Communication or dissemination",
                    "es": "Comunicación o difusión",
                    "pt": "Comunicação ou divulgação",
                    "ar": "الاتصال أو النشر"
                  }
                },
                {
                  "code": 3,
                  "label": {
                    "fr": "Coordination ou direction",
                    "en": "Coordination or management",
                    "es": "Coordinación o dirección",
                    "pt": "Coordenação ou direção",
                    "ar": "التنسيق أو الإدارة العليا"
                  }
                },
                {
                  "code": 4,
                  "label": {
                    "fr": "Méthodes ou qualité",
                    "en": "Methods or quality",
                    "es": "Métodos o calidad",
                    "pt": "Métodos ou qualidade",
                    "ar": "المنهجيات أو الجودة"
                  }
                },
                {
                  "code": 97,
                  "label": {
                    "fr": "Autre",
                    "en": "Other",
                    "es": "Otra",
                    "pt": "Outra",
                    "ar": "أخرى"
                  }
                }
              ],
              "source": "Rapport méthodologique du 15 novembre 2026, pp. 19–22, QI01",
              "gate": {
                "field": "QI01_Participation",
                "op": "in",
                "values": [
                  1
                ]
              }
            },
            {
              "id": "QI01_Fonction_Autre",
              "label": {
                "fr": "Autre : précisez",
                "en": "Other: please specify",
                "es": "Otra : especifique",
                "pt": "Outra : especifique",
                "ar": "أخرى : يرجى التحديد"
              },
              "type": "text",
              "options": [],
              "max_chars": 500,
              "gate": {
                "all": [
                  {
                    "field": "QI01_Participation",
                    "op": "in",
                    "values": [
                      1
                    ]
                  },
                  {
                    "field": "QI01_Fonction",
                    "op": "in",
                    "values": [
                      97
                    ]
                  }
                ]
              },
              "source": "Rapport méthodologique du 15 novembre 2026, pp. 19–22, QI01"
            },
            {
              "id": "QI01_Envoi",
              "label": {
                "fr": "Nature de l’envoi",
                "en": "Type of submission",
                "es": "Tipo de envío",
                "pt": "Tipo de envio",
                "ar": "نوع الإرسال"
              },
              "type": "single",
              "options": [
                {
                  "code": 1,
                  "label": {
                    "fr": "Première réponse",
                    "en": "First response",
                    "es": "Primera respuesta",
                    "pt": "Primeira resposta",
                    "ar": "الإجابة الأولى"
                  }
                },
                {
                  "code": 2,
                  "label": {
                    "fr": "Complément",
                    "en": "Supplement",
                    "es": "Complemento",
                    "pt": "Complemento",
                    "ar": "استكمال"
                  }
                }
              ],
              "source": "Rapport méthodologique du 15 novembre 2026, pp. 19–22, QI01",
              "gate": {
                "field": "QI01_Participation",
                "op": "in",
                "values": [
                  1
                ]
              }
            },
            {
              "id": "QI01_Reference_precedente",
              "label": {
                "fr": "Référence ou date de l’envoi précédent, si connue",
                "en": "Reference or date of the previous submission, if known",
                "es": "Referencia o fecha del envío anterior, si se conoce",
                "pt": "Referência ou data do envio anterior, se conhecida",
                "ar": "مرجع الإرسال السابق أو تاريخه، إن كان معروفا"
              },
              "type": "text",
              "options": [],
              "max_chars": 500,
              "gate": {
                "all": [
                  {
                    "field": "QI01_Participation",
                    "op": "in",
                    "values": [
                      1
                    ]
                  },
                  {
                    "field": "QI01_Envoi",
                    "op": "in",
                    "values": [
                      2
                    ]
                  }
                ]
              },
              "source": "Rapport méthodologique du 15 novembre 2026, pp. 19–22, QI01"
            },
            {
              "id": "QI01_Unite",
              "label": {
                "fr": "Service ou unité concernée",
                "en": "Service or unit concerned",
                "es": "Servicio o unidad correspondiente",
                "pt": "Serviço ou unidade em causa",
                "ar": "الإدارة أو الوحدة المعنية"
              },
              "type": "text",
              "options": [],
              "max_chars": 500,
              "source_extra": "Gestion de collecte — § 5.1 ; pas une question substantielle supplémentaire",
              "source": "Gestion de collecte — § 5.1 ; pas une question substantielle supplémentaire",
              "gate": {
                "field": "QI01_Participation",
                "op": "in",
                "values": [
                  1
                ]
              }
            },
            {
              "id": "QI01_Domaine",
              "label": {
                "fr": "Domaine ou secteur, facultatif",
                "en": "Domain or sector, optional",
                "es": "Ámbito o sector, opcional",
                "pt": "Domínio ou setor, facultativo",
                "ar": "المجال أو القطاع، اختياري"
              },
              "type": "text",
              "options": [],
              "max_chars": 500,
              "source_extra": "Gestion de collecte — § 5.1 ; pas une question substantielle supplémentaire",
              "source": "Gestion de collecte — § 5.1 ; pas une question substantielle supplémentaire",
              "gate": {
                "field": "QI01_Participation",
                "op": "in",
                "values": [
                  1
                ]
              }
            },
            {
              "id": "QI01_Langue_texte",
              "label": {
                "fr": "Langue principale de vos réponses libres",
                "en": "Main language of your free-text responses",
                "es": "Idioma principal de sus respuestas abiertas",
                "pt": "Língua principal das suas respostas abertas",
                "ar": "اللغة الأساسية لإجاباتك الحرة"
              },
              "type": "single",
              "options": [
                {
                  "code": "fr",
                  "label": {
                    "fr": "Français",
                    "en": "French",
                    "es": "Francés",
                    "pt": "Francês",
                    "ar": "الفرنسية"
                  }
                },
                {
                  "code": "en",
                  "label": {
                    "fr": "Anglais",
                    "en": "English",
                    "es": "Inglés",
                    "pt": "Inglês",
                    "ar": "الإنجليزية"
                  }
                },
                {
                  "code": "ar",
                  "label": {
                    "fr": "Arabe",
                    "en": "Arabic",
                    "es": "Árabe",
                    "pt": "Árabe",
                    "ar": "العربية"
                  }
                },
                {
                  "code": "pt",
                  "label": {
                    "fr": "Portugais",
                    "en": "Portuguese",
                    "es": "Portugués",
                    "pt": "Português",
                    "ar": "البرتغالية"
                  }
                },
                {
                  "code": "es",
                  "label": {
                    "fr": "Espagnol",
                    "en": "Spanish",
                    "es": "Español",
                    "pt": "Espanhol",
                    "ar": "الإسبانية"
                  }
                }
              ],
              "source_extra": "Gestion de collecte — § 5.1 ; pas une question substantielle supplémentaire",
              "source": "Gestion de collecte — § 5.1 ; pas une question substantielle supplémentaire",
              "gate": {
                "field": "QI01_Participation",
                "op": "in",
                "values": [
                  1
                ]
              }
            },
            {
              "id": "QI01_Origine_invitation",
              "label": {
                "fr": "Origine de l’invitation ou du lien",
                "en": "Origin of the invitation or link",
                "es": "Origen de la invitación o del enlace",
                "pt": "Origem do convite ou da ligação",
                "ar": "مصدر الدعوة أو الرابط"
              },
              "type": "single",
              "options": [
                {
                  "code": 1,
                  "label": {
                    "fr": "INS",
                    "en": "NSO",
                    "es": "ONE",
                    "pt": "INE",
                    "ar": "الجهاز الوطني للإحصاء"
                  }
                },
                {
                  "code": 2,
                  "label": {
                    "fr": "STATAFRIC ou consultant",
                    "en": "STATAFRIC or consultant",
                    "es": "STATAFRIC o consultor",
                    "pt": "STATAFRIC ou consultor",
                    "ar": "STATAFRIC أو الاستشاري"
                  }
                },
                {
                  "code": 3,
                  "label": {
                    "fr": "Autre relais",
                    "en": "Other forwarding organisation",
                    "es": "Otra entidad que transmitió la invitación",
                    "pt": "Outra entidade que encaminhou o convite",
                    "ar": "جهة أخرى أحالت الدعوة"
                  }
                },
                {
                  "code": 4,
                  "label": {
                    "fr": "Accès spontané",
                    "en": "Unsolicited access",
                    "es": "Acceso espontáneo",
                    "pt": "Acesso espontâneo",
                    "ar": "وصول تلقائي دون دعوة مباشرة"
                  }
                },
                {
                  "code": 98,
                  "label": {
                    "fr": "Ne sait pas",
                    "en": "Do not know",
                    "es": "No sabe",
                    "pt": "Não sabe",
                    "ar": "لا أعرف"
                  }
                }
              ],
              "source_extra": "Gestion de collecte — § 5.1 ; pas une question substantielle supplémentaire",
              "source": "Gestion de collecte — § 5.1 ; pas une question substantielle supplémentaire",
              "gate": {
                "field": "QI01_Participation",
                "op": "in",
                "values": [
                  1
                ]
              }
            },
            {
              "id": "QI01_INS_relais",
              "label": {
                "fr": "INS ou organisme relais, facultatif",
                "en": "Forwarding NSO or organisation, optional",
                "es": "ONE o entidad que transmitió la invitación, opcional",
                "pt": "INE ou entidade que encaminhou o convite, facultativo",
                "ar": "الجهاز الوطني للإحصاء أو الجهة المحيلة، اختياري"
              },
              "type": "text",
              "options": [],
              "max_chars": 500,
              "source_extra": "Gestion de collecte — § 5.1 ; pas une question substantielle supplémentaire",
              "source": "Gestion de collecte — § 5.1 ; pas une question substantielle supplémentaire",
              "gate": {
                "field": "QI01_Participation",
                "op": "in",
                "values": [
                  1
                ]
              }
            }
          ],
          "note": {
            "fr": "Une réponse consolidée par INS est privilégiée. Pour une contribution de service ou individuelle, répondre sur le périmètre réellement connu et utiliser « ne sait pas » pour le reste de l’INS.",
            "en": "A consolidated response per NSO is preferred. For a service-level or individual contribution, describe only the scope you actually know and use ‘Do not know’ for the rest of the NSO.",
            "es": "Se prefiere una respuesta consolidada por ONE. En una contribución de servicio o individual, describa únicamente el ámbito que conoce y utilice « No sabe » para el resto de la ONE.",
            "pt": "É preferível uma resposta consolidada por INE. Numa contribuição de serviço ou individual, descreva apenas o âmbito que conhece e use « Não sabe » para o restante INE.",
            "ar": "يفضل تقديم إجابة موحدة لكل جهاز وطني للإحصاء. عند تقديم مساهمة من إدارة أو فرد، صف فقط النطاق الذي تعرفه فعليا، واختر « لا أعرف » فيما يتعلق ببقية الجهاز."
          },
          "gate": null,
          "theme": "Suivi et moyens",
          "source": "Rapport méthodologique du 15 novembre 2026, pp. 19–22, QI01"
        },
        {
          "id": "QI02",
          "title": {
            "fr": "Demandes reçues pendant les douze mois de référence",
            "en": "Requests received during the twelve-month reference period",
            "es": "Solicitudes recibidas durante los doce meses de referencia",
            "pt": "Pedidos recebidos nos doze meses de referência",
            "ar": "الطلبات المستلمة خلال الاثني عشر شهرا المرجعية"
          },
          "fields": [
            {
              "id": "QI02a",
              "label": {
                "fr": "Combien de demandes distinctes avez-vous recensées dans le périmètre de votre réponse ?",
                "en": "How many distinct requests did you identify within the scope of your response?",
                "es": "¿Cuántas solicitudes distintas ha identificado en el ámbito de su respuesta?",
                "pt": "Quantos pedidos distintos identificou no âmbito da sua resposta?",
                "ar": "كم طلبا منفصلا أحصيتم ضمن نطاق إجابتكم؟"
              },
              "type": "single",
              "options": [
                {
                  "code": 0,
                  "label": {
                    "fr": "Aucune / aucun",
                    "en": "None",
                    "es": "Ninguna",
                    "pt": "Nenhum",
                    "ar": "لا يوجد"
                  }
                },
                {
                  "code": 1,
                  "label": {
                    "fr": "De 1 à 5",
                    "en": "1 to 5",
                    "es": "De 1 a 5",
                    "pt": "De 1 a 5",
                    "ar": "من 1 إلى 5"
                  }
                },
                {
                  "code": 2,
                  "label": {
                    "fr": "De 6 à 20",
                    "en": "6 to 20",
                    "es": "De 6 a 20",
                    "pt": "De 6 a 20",
                    "ar": "من 6 إلى 20"
                  }
                },
                {
                  "code": 3,
                  "label": {
                    "fr": "De 21 à 50",
                    "en": "21 to 50",
                    "es": "De 21 a 50",
                    "pt": "De 21 a 50",
                    "ar": "من 21 إلى 50"
                  }
                },
                {
                  "code": 4,
                  "label": {
                    "fr": "Plus de 50",
                    "en": "More than 50",
                    "es": "Más de 50",
                    "pt": "Mais de 50",
                    "ar": "أكثر من 50"
                  }
                },
                {
                  "code": 98,
                  "label": {
                    "fr": "Ne sait pas",
                    "en": "Do not know",
                    "es": "No sabe",
                    "pt": "Não sabe",
                    "ar": "لا أعرف"
                  }
                }
              ],
              "source": "Rapport méthodologique du 15 novembre 2026, pp. 19–22, QI02",
              "gate": {
                "field": "QI01_Participation",
                "op": "in",
                "values": [
                  1
                ]
              }
            },
            {
              "id": "QI02b",
              "label": {
                "fr": "Certaines de ces demandes exigeaient-elles une réponse dans les 24 heures ou moins suivant leur réception ?",
                "en": "Did any of these requests require a response within 24 hours of receipt?",
                "es": "¿Alguna de estas solicitudes exigía respuesta en las 24 horas siguientes a su recepción?",
                "pt": "Algum destes pedidos exigia resposta nas 24 horas seguintes à receção?",
                "ar": "هل تطلب أي من هذه الطلبات إجابة خلال 24 ساعة أو أقل من استلامه؟"
              },
              "type": "single",
              "options": [
                {
                  "code": 1,
                  "label": {
                    "fr": "Oui",
                    "en": "Yes",
                    "es": "Sí",
                    "pt": "Sim",
                    "ar": "نعم"
                  }
                },
                {
                  "code": 0,
                  "label": {
                    "fr": "Non",
                    "en": "No",
                    "es": "No",
                    "pt": "Não",
                    "ar": "لا"
                  }
                },
                {
                  "code": 98,
                  "label": {
                    "fr": "Ne sait pas",
                    "en": "Do not know",
                    "es": "No sabe",
                    "pt": "Não sabe",
                    "ar": "لا أعرف"
                  }
                },
                {
                  "code": 99,
                  "label": {
                    "fr": "Non applicable, aucune demande",
                    "en": "Not applicable, no requests",
                    "es": "No aplicable, ninguna solicitud",
                    "pt": "Não aplicável, nenhum pedido",
                    "ar": "لا ينطبق، لا توجد طلبات"
                  }
                }
              ],
              "source": "Rapport méthodologique du 15 novembre 2026, pp. 19–22, QI02",
              "gate": {
                "field": "QI01_Participation",
                "op": "in",
                "values": [
                  1
                ]
              }
            },
            {
              "id": "QI02c",
              "label": {
                "fr": "Sur quoi repose cette réponse ?",
                "en": "What is this response based on?",
                "es": "¿En qué se basa esta respuesta?",
                "pt": "Em que se baseia esta resposta?",
                "ar": "على ماذا تستند هذه الإجابة؟"
              },
              "type": "single",
              "options": [
                {
                  "code": 1,
                  "label": {
                    "fr": "Registre ou inventaire consulté",
                    "en": "Register or inventory consulted",
                    "es": "Registro o inventario consultado",
                    "pt": "Registo ou inventário consultado",
                    "ar": "سجل أو حصر تمت مراجعته"
                  }
                },
                {
                  "code": 2,
                  "label": {
                    "fr": "Estimation fondée sur la pratique connue",
                    "en": "Estimate based on known practice",
                    "es": "Estimación basada en la práctica conocida",
                    "pt": "Estimativa baseada na prática conhecida",
                    "ar": "تقدير يستند إلى الممارسة المعروفة"
                  }
                },
                {
                  "code": 98,
                  "label": {
                    "fr": "Ne sait pas",
                    "en": "Do not know",
                    "es": "No sabe",
                    "pt": "Não sabe",
                    "ar": "لا أعرف"
                  }
                }
              ],
              "source": "Rapport méthodologique du 15 novembre 2026, pp. 19–22, QI02",
              "gate": {
                "field": "QI01_Participation",
                "op": "in",
                "values": [
                  1
                ]
              }
            }
          ],
          "note": {
            "fr": "Une demande désigne un appui sollicité pour préparer un discours, une allocution ou une intervention orale d’un décideur. Plusieurs échanges ou versions concernant le même appui comptent comme une seule demande. Les demandes générales de données sans lien connu avec un discours sont exclues. Si aucune demande n’a été reçue, passer à QI08. Si le nombre est inconnu, QI03 peut être renseignée lorsqu’un cas réel est connu.",
            "en": "A request means support requested to prepare a decision-maker’s speech, address or oral intervention. Multiple exchanges or versions for the same support count as one request. General data requests with no known link to a speech are excluded. If there were no requests, proceed to QI08. If the number is unknown, QI03 may still be completed when a real case is known.",
            "es": "Una solicitud es un apoyo requerido para preparar un discurso, una alocución o una intervención oral de un decisor. Los intercambios o versiones del mismo apoyo cuentan como una sola solicitud. Se excluyen las peticiones generales de datos sin vínculo conocido con un discurso. Si no hubo solicitudes, pase a QI08. Si desconoce el número, puede responder QI03 cuando conozca un caso real.",
            "pt": "Um pedido é um apoio solicitado para preparar um discurso, uma alocução ou uma intervenção oral de um decisor. Várias trocas ou versões do mesmo apoio contam como um único pedido. Excluem-se pedidos gerais de dados sem ligação conhecida a um discurso. Se não houve pedidos, avance para QI08. Se o número for desconhecido, QI03 pode ser preenchida quando se conhece um caso real.",
            "ar": "الطلب هو دعم مطلوب لإعداد خطاب أو كلمة أو مداخلة شفهية لصانع قرار. تعد المراسلات أو النسخ المتعلقة بالدعم نفسه طلبا واحدا. تستبعد طلبات البيانات العامة غير المرتبطة بخطاب معروف. إذا لم توجد طلبات، انتقل إلى QI08. وإذا كان العدد مجهولا، يمكن الإجابة عن QI03 عند معرفة حالة فعلية."
          },
          "gate": null,
          "theme": "Demande et urgence",
          "source": "Rapport méthodologique du 15 novembre 2026, pp. 19–22, QI02"
        },
        {
          "id": "QI03",
          "title": {
            "fr": "Dernière demande clôturée connue",
            "en": "Most recent known closed request",
            "es": "Última solicitud cerrada conocida",
            "pt": "Último pedido encerrado conhecido",
            "ar": "أحدث طلب مغلق معروف"
          },
          "fields": [
            {
              "id": "QI03_Situation",
              "label": {
                "fr": "Situation",
                "en": "Situation",
                "es": "Situación",
                "pt": "Situação",
                "ar": "الحالة"
              },
              "type": "single",
              "options": [
                {
                  "code": 1,
                  "label": {
                    "fr": "Des chiffres ont été transmis",
                    "en": "Figures were provided",
                    "es": "Se proporcionaron cifras",
                    "pt": "Foram fornecidos números",
                    "ar": "تم تقديم أرقام"
                  }
                },
                {
                  "code": 2,
                  "label": {
                    "fr": "La demande a été clôturée sans fourniture de chiffres",
                    "en": "The request was closed without providing figures",
                    "es": "La solicitud se cerró sin proporcionar cifras",
                    "pt": "O pedido foi encerrado sem fornecimento de números",
                    "ar": "أغلق الطلب دون تقديم أرقام"
                  }
                },
                {
                  "code": 3,
                  "label": {
                    "fr": "Aucune demande clôturée connue pendant la période",
                    "en": "No known request closed during the period",
                    "es": "Ninguna solicitud cerrada conocida durante el período",
                    "pt": "Nenhum pedido encerrado conhecido durante o período",
                    "ar": "لا أعرف طلبا أغلق خلال الفترة"
                  }
                },
                {
                  "code": 98,
                  "label": {
                    "fr": "Je ne dispose pas des informations nécessaires",
                    "en": "I do not have the necessary information",
                    "es": "No dispongo de la información necesaria",
                    "pt": "Não disponho das informações necessárias",
                    "ar": "لا تتوفر لدي المعلومات اللازمة"
                  }
                }
              ],
              "source": "Rapport méthodologique du 15 novembre 2026, pp. 19–22, QI03",
              "gate": {
                "field": "QI01_Participation",
                "op": "in",
                "values": [
                  1
                ]
              }
            },
            {
              "id": "QI03_Mois",
              "label": {
                "fr": "Mois et année de clôture, MM/AAAA",
                "en": "Month and year of closure, MM/YYYY",
                "es": "Mes y año de cierre, MM/AAAA",
                "pt": "Mês e ano do encerramento, MM/AAAA",
                "ar": "شهر وسنة الإغلاق، MM/YYYY"
              },
              "type": "month",
              "options": [],
              "max_chars": 7,
              "gate": {
                "all": [
                  {
                    "field": "QI01_Participation",
                    "op": "in",
                    "values": [
                      1
                    ]
                  },
                  {
                    "field": "QI03_Situation",
                    "op": "in",
                    "values": [
                      1,
                      2
                    ]
                  }
                ]
              },
              "source": "Rapport méthodologique du 15 novembre 2026, pp. 19–22, QI03"
            },
            {
              "id": "QI03_Theme",
              "label": {
                "fr": "Thème général, sans nom de personne ni titre confidentiel du discours",
                "en": "General topic, without a person’s name or a confidential speech title",
                "es": "Tema general, sin nombre de persona ni título confidencial del discurso",
                "pt": "Tema geral, sem nome de pessoa nem título confidencial do discurso",
                "ar": "الموضوع العام، دون اسم شخص أو عنوان سري للخطاب"
              },
              "type": "text",
              "options": [],
              "max_chars": 500,
              "gate": {
                "all": [
                  {
                    "field": "QI01_Participation",
                    "op": "in",
                    "values": [
                      1
                    ]
                  },
                  {
                    "field": "QI03_Situation",
                    "op": "in",
                    "values": [
                      1,
                      2
                    ]
                  }
                ]
              },
              "source": "Rapport méthodologique du 15 novembre 2026, pp. 19–22, QI03"
            },
            {
              "id": "QI03_Demandeur",
              "label": {
                "fr": "Service demandeur",
                "en": "Requesting service",
                "es": "Servicio solicitante",
                "pt": "Serviço requerente",
                "ar": "الإدارة الطالبة"
              },
              "type": "single",
              "options": [
                {
                  "code": 1,
                  "label": {
                    "fr": "Cabinet ou équipe d’un décideur national",
                    "en": "Office or team of a national decision-maker",
                    "es": "Gabinete o equipo de un decisor nacional",
                    "pt": "Gabinete ou equipa de um decisor nacional",
                    "ar": "مكتب أو فريق صانع قرار وطني"
                  }
                },
                {
                  "code": 2,
                  "label": {
                    "fr": "Ministère ou administration sectorielle",
                    "en": "Ministry or sectoral administration",
                    "es": "Ministerio o administración sectorial",
                    "pt": "Ministério ou administração setorial",
                    "ar": "وزارة أو إدارة قطاعية"
                  }
                },
                {
                  "code": 3,
                  "label": {
                    "fr": "Autorité territoriale ou locale",
                    "en": "Subnational or local authority",
                    "es": "Autoridad territorial o local",
                    "pt": "Autoridade territorial ou local",
                    "ar": "سلطة إقليمية أو محلية"
                  }
                },
                {
                  "code": 4,
                  "label": {
                    "fr": "Direction de l’INS",
                    "en": "NSO management",
                    "es": "Dirección de la ONE",
                    "pt": "Direção do INE",
                    "ar": "إدارة الجهاز الوطني للإحصاء"
                  }
                },
                {
                  "code": 5,
                  "label": {
                    "fr": "Institution régionale ou internationale",
                    "en": "Regional or international institution",
                    "es": "Institución regional o internacional",
                    "pt": "Instituição regional ou internacional",
                    "ar": "مؤسسة إقليمية أو دولية"
                  }
                },
                {
                  "code": 97,
                  "label": {
                    "fr": "Autre",
                    "en": "Other",
                    "es": "Otra",
                    "pt": "Outra",
                    "ar": "أخرى"
                  }
                },
                {
                  "code": 98,
                  "label": {
                    "fr": "Ne sait pas",
                    "en": "Do not know",
                    "es": "No sabe",
                    "pt": "Não sabe",
                    "ar": "لا أعرف"
                  }
                }
              ],
              "gate": {
                "all": [
                  {
                    "field": "QI01_Participation",
                    "op": "in",
                    "values": [
                      1
                    ]
                  },
                  {
                    "field": "QI03_Situation",
                    "op": "in",
                    "values": [
                      1,
                      2
                    ]
                  }
                ]
              },
              "source": "Rapport méthodologique du 15 novembre 2026, pp. 19–22, QI03"
            },
            {
              "id": "QI03_Demandeur_Autre",
              "label": {
                "fr": "Autre : précisez",
                "en": "Other: please specify",
                "es": "Otra : especifique",
                "pt": "Outra : especifique",
                "ar": "أخرى : يرجى التحديد"
              },
              "type": "text",
              "options": [],
              "max_chars": 500,
              "gate": {
                "all": [
                  {
                    "field": "QI01_Participation",
                    "op": "in",
                    "values": [
                      1
                    ]
                  },
                  {
                    "field": "QI03_Demandeur",
                    "op": "in",
                    "values": [
                      97
                    ]
                  }
                ]
              },
              "source": "Rapport méthodologique du 15 novembre 2026, pp. 19–22, QI03"
            },
            {
              "id": "QI03_Canal",
              "label": {
                "fr": "Canal principal de réception",
                "en": "Main channel through which the request was received",
                "es": "Canal principal de recepción de la solicitud",
                "pt": "Principal canal de receção do pedido",
                "ar": "القناة الرئيسية لاستلام الطلب"
              },
              "type": "single",
              "options": [
                {
                  "code": 1,
                  "label": {
                    "fr": "Courriel",
                    "en": "Email",
                    "es": "Correo electrónico",
                    "pt": "Correio eletrónico",
                    "ar": "البريد الإلكتروني"
                  }
                },
                {
                  "code": 2,
                  "label": {
                    "fr": "Courrier ou note officielle",
                    "en": "Letter or official memorandum",
                    "es": "Carta o nota oficial",
                    "pt": "Carta ou nota oficial",
                    "ar": "رسالة أو مذكرة رسمية"
                  }
                },
                {
                  "code": 3,
                  "label": {
                    "fr": "Téléphone ou échange oral",
                    "en": "Telephone or oral exchange",
                    "es": "Teléfono o intercambio oral",
                    "pt": "Telefone ou troca oral",
                    "ar": "الهاتف أو التواصل الشفهي"
                  }
                },
                {
                  "code": 4,
                  "label": {
                    "fr": "Messagerie instantanée",
                    "en": "Instant messaging",
                    "es": "Mensajería instantánea",
                    "pt": "Mensagens instantâneas",
                    "ar": "المراسلة الفورية"
                  }
                },
                {
                  "code": 5,
                  "label": {
                    "fr": "Formulaire ou plateforme",
                    "en": "Form or platform",
                    "es": "Formulario o plataforma",
                    "pt": "Formulário ou plataforma",
                    "ar": "استمارة أو منصة"
                  }
                },
                {
                  "code": 97,
                  "label": {
                    "fr": "Autre",
                    "en": "Other",
                    "es": "Otra",
                    "pt": "Outra",
                    "ar": "أخرى"
                  }
                },
                {
                  "code": 98,
                  "label": {
                    "fr": "Ne sait pas",
                    "en": "Do not know",
                    "es": "No sabe",
                    "pt": "Não sabe",
                    "ar": "لا أعرف"
                  }
                }
              ],
              "gate": {
                "all": [
                  {
                    "field": "QI01_Participation",
                    "op": "in",
                    "values": [
                      1
                    ]
                  },
                  {
                    "field": "QI03_Situation",
                    "op": "in",
                    "values": [
                      1,
                      2
                    ]
                  }
                ]
              },
              "source": "Rapport méthodologique du 15 novembre 2026, pp. 19–22, QI03"
            },
            {
              "id": "QI03_Canal_Autre",
              "label": {
                "fr": "Autre : précisez",
                "en": "Other: please specify",
                "es": "Otra : especifique",
                "pt": "Outra : especifique",
                "ar": "أخرى : يرجى التحديد"
              },
              "type": "text",
              "options": [],
              "max_chars": 500,
              "gate": {
                "all": [
                  {
                    "field": "QI01_Participation",
                    "op": "in",
                    "values": [
                      1
                    ]
                  },
                  {
                    "field": "QI03_Canal",
                    "op": "in",
                    "values": [
                      97
                    ]
                  }
                ]
              },
              "source": "Rapport méthodologique du 15 novembre 2026, pp. 19–22, QI03"
            }
          ],
          "note": {
            "fr": "Choisir la demande la plus récemment clôturée pendant la période de référence, qu’elle ait ou non abouti à la fourniture de chiffres. Ne pas sélectionner uniquement un exemple particulièrement réussi. Les situations 3 et 98 conduisent à QI08. La situation 2 conduit à QI04, puis QI08.",
            "en": "Select the request most recently closed during the reference period, whether or not figures were provided. Do not select only a particularly successful example. Situations 3 and 98 lead to QI08. Situation 2 leads to QI04 and then QI08.",
            "es": "Seleccione la solicitud cerrada más recientemente durante el período de referencia, se hayan proporcionado cifras o no. No elija únicamente un caso especialmente exitoso. Las situaciones 3 y 98 llevan a QI08. La situación 2 lleva a QI04 y después a QI08.",
            "pt": "Selecione o pedido encerrado mais recentemente no período de referência, tenham sido fornecidos números ou não. Não escolha apenas um caso particularmente bem-sucedido. As situações 3 e 98 levam a QI08. A situação 2 leva a QI04 e depois a QI08.",
            "ar": "اختر أحدث طلب أغلق خلال الفترة المرجعية، سواء قدمت أرقام أم لا. لا تختَر حالة ناجحة بشكل خاص فقط. تؤدي الحالتان 3 و98 إلى QI08، وتؤدي الحالة 2 إلى QI04 ثم QI08."
          },
          "gate": {
            "field": "QI02a",
            "op": "not_in",
            "values": [
              0
            ]
          },
          "theme": "Demande et urgence",
          "source": "Rapport méthodologique du 15 novembre 2026, pp. 19–22, QI03"
        },
        {
          "id": "QI04",
          "title": {
            "fr": "Délai et traitement de cette dernière demande",
            "en": "Deadline and handling of this most recent request",
            "es": "Plazo y tratamiento de esta última solicitud",
            "pt": "Prazo e tratamento deste último pedido",
            "ar": "مهلة هذا الطلب الأخير ومعالجته"
          },
          "fields": [
            {
              "id": "QI04a",
              "label": {
                "fr": "Quel délai était laissé à l’INS pour répondre, à partir de la réception de la demande ?",
                "en": "How much time was the NSO given to respond from receipt of the request?",
                "es": "¿Cuánto tiempo se concedió a la ONE para responder desde la recepción de la solicitud?",
                "pt": "Quanto tempo foi concedido ao INE para responder a partir da receção do pedido?",
                "ar": "ما المهلة التي أتيحت للجهاز الوطني للإحصاء للإجابة منذ استلام الطلب؟"
              },
              "type": "single",
              "options": [
                {
                  "code": 1,
                  "label": {
                    "fr": "Moins de 6 heures",
                    "en": "Less than 6 hours",
                    "es": "Menos de 6 horas",
                    "pt": "Menos de 6 horas",
                    "ar": "أقل من 6 ساعات"
                  }
                },
                {
                  "code": 2,
                  "label": {
                    "fr": "De 6 à 24 heures incluses",
                    "en": "6 to 24 hours inclusive",
                    "es": "De 6 a 24 horas, inclusive",
                    "pt": "De 6 a 24 horas, inclusive",
                    "ar": "من 6 إلى 24 ساعة شاملة"
                  }
                },
                {
                  "code": 3,
                  "label": {
                    "fr": "Plus de 24 heures à 3 jours inclus",
                    "en": "More than 24 hours up to 3 days inclusive",
                    "es": "Más de 24 horas y hasta 3 días, inclusive",
                    "pt": "Mais de 24 horas até 3 dias, inclusive",
                    "ar": "أكثر من 24 ساعة وحتى 3 أيام شاملة"
                  }
                },
                {
                  "code": 4,
                  "label": {
                    "fr": "Plus de 3 jours",
                    "en": "More than 3 days",
                    "es": "Más de 3 días",
                    "pt": "Mais de 3 dias",
                    "ar": "أكثر من 3 أيام"
                  }
                },
                {
                  "code": 5,
                  "label": {
                    "fr": "Aucune échéance indiquée",
                    "en": "No deadline specified",
                    "es": "Sin plazo especificado",
                    "pt": "Sem prazo indicado",
                    "ar": "لم تحدد مهلة"
                  }
                },
                {
                  "code": 98,
                  "label": {
                    "fr": "Ne sait pas",
                    "en": "Do not know",
                    "es": "No sabe",
                    "pt": "Não sabe",
                    "ar": "لا أعرف"
                  }
                }
              ],
              "source": "Rapport méthodologique du 15 novembre 2026, pp. 19–22, QI04",
              "gate": {
                "field": "QI01_Participation",
                "op": "in",
                "values": [
                  1
                ]
              }
            },
            {
              "id": "QI04b",
              "label": {
                "fr": "La réponse de l’INS a-t-elle été transmise avant l’échéance demandée ?",
                "en": "Was the NSO’s response sent before the requested deadline?",
                "es": "¿Se envió la respuesta de la ONE antes del plazo solicitado?",
                "pt": "A resposta do INE foi enviada antes do prazo solicitado?",
                "ar": "هل أرسلت إجابة الجهاز الوطني للإحصاء قبل الموعد المطلوب؟"
              },
              "type": "single",
              "options": [
                {
                  "code": 1,
                  "label": {
                    "fr": "Oui",
                    "en": "Yes",
                    "es": "Sí",
                    "pt": "Sim",
                    "ar": "نعم"
                  }
                },
                {
                  "code": 0,
                  "label": {
                    "fr": "Non",
                    "en": "No",
                    "es": "No",
                    "pt": "Não",
                    "ar": "لا"
                  }
                },
                {
                  "code": 98,
                  "label": {
                    "fr": "Ne sait pas",
                    "en": "Do not know",
                    "es": "No sabe",
                    "pt": "Não sabe",
                    "ar": "لا أعرف"
                  }
                },
                {
                  "code": 99,
                  "label": {
                    "fr": "Non applicable, aucune échéance indiquée",
                    "en": "Not applicable, no deadline specified",
                    "es": "No aplicable, sin plazo especificado",
                    "pt": "Não aplicável, sem prazo indicado",
                    "ar": "لا ينطبق، لم تحدد مهلة"
                  }
                }
              ],
              "source": "Rapport méthodologique du 15 novembre 2026, pp. 19–22, QI04",
              "gate": {
                "field": "QI01_Participation",
                "op": "in",
                "values": [
                  1
                ]
              }
            },
            {
              "id": "QI04c",
              "label": {
                "fr": "Si aucun chiffre n’a été fourni ou si le délai n’a pas été respecté, quel a été le principal obstacle ?",
                "en": "If no figures were provided or the deadline was missed, what was the main obstacle?",
                "es": "Si no se proporcionaron cifras o se incumplió el plazo, ¿cuál fue el principal obstáculo?",
                "pt": "Se não foram fornecidos números ou se o prazo não foi cumprido, qual foi o principal obstáculo?",
                "ar": "إذا لم تقدم أرقام أو لم تحترم المهلة، فما العائق الرئيسي؟"
              },
              "type": "single",
              "options": [
                {
                  "code": 1,
                  "label": {
                    "fr": "Donnée appropriée indisponible",
                    "en": "Suitable data unavailable",
                    "es": "Datos adecuados no disponibles",
                    "pt": "Dados adequados indisponíveis",
                    "ar": "عدم توفر بيانات مناسبة"
                  }
                },
                {
                  "code": 2,
                  "label": {
                    "fr": "Besoin insuffisamment précisé",
                    "en": "Need insufficiently specified",
                    "es": "Necesidad insuficientemente precisada",
                    "pt": "Necessidade insuficientemente especificada",
                    "ar": "عدم تحديد الحاجة بما يكفي"
                  }
                },
                {
                  "code": 3,
                  "label": {
                    "fr": "Délai trop court",
                    "en": "Deadline too short",
                    "es": "Plazo demasiado corto",
                    "pt": "Prazo demasiado curto",
                    "ar": "قصر المهلة الشديد"
                  }
                },
                {
                  "code": 4,
                  "label": {
                    "fr": "Contrôle ou validation non terminé",
                    "en": "Checking or validation not completed",
                    "es": "Control o validación sin finalizar",
                    "pt": "Controlo ou validação não concluídos",
                    "ar": "عدم اكتمال المراجعة أو التحقق"
                  }
                },
                {
                  "code": 5,
                  "label": {
                    "fr": "Agent ou service compétent indisponible",
                    "en": "Responsible staff member or service unavailable",
                    "es": "Agente o servicio competente no disponible",
                    "pt": "Agente ou serviço competente indisponível",
                    "ar": "عدم توفر الموظف أو الإدارة المختصة"
                  }
                },
                {
                  "code": 97,
                  "label": {
                    "fr": "Autre",
                    "en": "Other",
                    "es": "Otra",
                    "pt": "Outra",
                    "ar": "أخرى"
                  }
                },
                {
                  "code": 98,
                  "label": {
                    "fr": "Ne sait pas",
                    "en": "Do not know",
                    "es": "No sabe",
                    "pt": "Não sabe",
                    "ar": "لا أعرف"
                  }
                },
                {
                  "code": 99,
                  "label": {
                    "fr": "Non applicable",
                    "en": "Not applicable",
                    "es": "No aplicable",
                    "pt": "Não aplicável",
                    "ar": "لا ينطبق"
                  }
                }
              ],
              "source": "Rapport méthodologique du 15 novembre 2026, pp. 19–22, QI04",
              "gate": {
                "field": "QI01_Participation",
                "op": "in",
                "values": [
                  1
                ]
              }
            },
            {
              "id": "QI04c_Autre",
              "label": {
                "fr": "Autre : précisez",
                "en": "Other: please specify",
                "es": "Otra : especifique",
                "pt": "Outra : especifique",
                "ar": "أخرى : يرجى التحديد"
              },
              "type": "text",
              "options": [],
              "max_chars": 500,
              "gate": {
                "all": [
                  {
                    "field": "QI01_Participation",
                    "op": "in",
                    "values": [
                      1
                    ]
                  },
                  {
                    "field": "QI04c",
                    "op": "in",
                    "values": [
                      97
                    ]
                  }
                ]
              },
              "source": "Rapport méthodologique du 15 novembre 2026, pp. 19–22, QI04"
            }
          ],
          "note": {
            "fr": "Les durées sont calendaires, y compris les nuits et week-ends. Une réponse expliquant qu’aucun chiffre ne pouvait être fourni reste distinguée de la fourniture de chiffres par QI03. Le respect d’un délai ne suffit pas à conclure que le besoin statistique a été satisfait.",
            "en": "Times are calendar durations, including nights and weekends. QI03 distinguishes a response explaining that no figures could be provided from an actual provision of figures. Meeting a deadline does not by itself establish that the statistical need was met.",
            "es": "Los plazos son naturales e incluyen noches y fines de semana. QI03 distingue una respuesta que explica que no se podían facilitar cifras de una entrega efectiva de cifras. Cumplir un plazo no demuestra por sí solo que se haya satisfecho la necesidad estadística.",
            "pt": "Os prazos são de calendário e incluem noites e fins de semana. QI03 distingue uma resposta explicando a impossibilidade de fornecer números do fornecimento efetivo. Cumprir um prazo não demonstra, por si só, que a necessidade estatística foi satisfeita.",
            "ar": "المهل تقويمية وتشمل الليالي وعطلات نهاية الأسبوع. يميز QI03 بين الإجابة التي توضح تعذر تقديم أرقام وبين تقديم أرقام فعليا. لا يكفي احترام المهلة وحده للاستنتاج بأن الحاجة الإحصائية قد لبيت."
          },
          "gate": {
            "all": [
              {
                "field": "QI02a",
                "op": "not_in",
                "values": [
                  0
                ]
              },
              {
                "field": "QI03_Situation",
                "op": "in",
                "values": [
                  1,
                  2
                ]
              }
            ]
          },
          "theme": "Demande et urgence",
          "source": "Rapport méthodologique du 15 novembre 2026, pp. 19–22, QI04"
        },
        {
          "id": "QI05",
          "title": {
            "fr": "Source et informations accompagnant le premier chiffre transmis",
            "en": "Source and information accompanying the first figure provided",
            "es": "Fuente e información que acompañan a la primera cifra facilitada",
            "pt": "Fonte e informações que acompanham o primeiro número fornecido",
            "ar": "مصدر أول رقم مقدم والمعلومات المصاحبة له"
          },
          "fields": [
            {
              "id": "QI05_Origine",
              "label": {
                "fr": "Origine principale de ce chiffre",
                "en": "Main origin of this figure",
                "es": "Origen principal de esta cifra",
                "pt": "Origem principal deste número",
                "ar": "المصدر الرئيسي لهذا الرقم"
              },
              "type": "single",
              "options": [
                {
                  "code": 1,
                  "label": {
                    "fr": "Publication, base ou résultat produit par l’INS",
                    "en": "Publication, database or result produced by the NSO",
                    "es": "Publicación, base o resultado producido por la ONE",
                    "pt": "Publicação, base ou resultado produzido pelo INE",
                    "ar": "منشور أو قاعدة بيانات أو نتيجة أنتجها الجهاز الوطني للإحصاء"
                  }
                },
                {
                  "code": 2,
                  "label": {
                    "fr": "Source officielle d’une autre administration nationale",
                    "en": "Official source from another national administration",
                    "es": "Fuente oficial de otra administración nacional",
                    "pt": "Fonte oficial de outra administração nacional",
                    "ar": "مصدر رسمي لإدارة وطنية أخرى"
                  }
                },
                {
                  "code": 3,
                  "label": {
                    "fr": "Organisation régionale ou internationale",
                    "en": "Regional or international organisation",
                    "es": "Organización regional o internacional",
                    "pt": "Organização regional ou internacional",
                    "ar": "منظمة إقليمية أو دولية"
                  }
                },
                {
                  "code": 4,
                  "label": {
                    "fr": "Recherche ou source privée identifiée",
                    "en": "Identified research or private source",
                    "es": "Investigación o fuente privada identificada",
                    "pt": "Investigação ou fonte privada identificada",
                    "ar": "بحث أو مصدر خاص محدد"
                  }
                },
                {
                  "code": 5,
                  "label": {
                    "fr": "Discours ou document antérieur sans retour à la source statistique",
                    "en": "Earlier speech or document, without consulting the statistical source",
                    "es": "Discurso o documento anterior sin consultar la fuente estadística",
                    "pt": "Discurso ou documento anterior sem consultar a fonte estatística",
                    "ar": "خطاب أو وثيقة سابقة دون الرجوع إلى المصدر الإحصائي"
                  }
                },
                {
                  "code": 6,
                  "label": {
                    "fr": "Source non identifiée",
                    "en": "Unidentified source",
                    "es": "Fuente no identificada",
                    "pt": "Fonte não identificada",
                    "ar": "مصدر غير محدد"
                  }
                },
                {
                  "code": 97,
                  "label": {
                    "fr": "Autre",
                    "en": "Other",
                    "es": "Otra",
                    "pt": "Outra",
                    "ar": "أخرى"
                  }
                },
                {
                  "code": 98,
                  "label": {
                    "fr": "Ne sait pas",
                    "en": "Do not know",
                    "es": "No sabe",
                    "pt": "Não sabe",
                    "ar": "لا أعرف"
                  }
                }
              ],
              "source": "Rapport méthodologique du 15 novembre 2026, pp. 19–22, QI05",
              "gate": {
                "field": "QI01_Participation",
                "op": "in",
                "values": [
                  1
                ]
              }
            },
            {
              "id": "QI05_Origine_Autre",
              "label": {
                "fr": "Autre : précisez",
                "en": "Other: please specify",
                "es": "Otra : especifique",
                "pt": "Outra : especifique",
                "ar": "أخرى : يرجى التحديد"
              },
              "type": "text",
              "options": [],
              "max_chars": 500,
              "gate": {
                "all": [
                  {
                    "field": "QI01_Participation",
                    "op": "in",
                    "values": [
                      1
                    ]
                  },
                  {
                    "field": "QI05_Origine",
                    "op": "in",
                    "values": [
                      97
                    ]
                  }
                ]
              },
              "source": "Rapport méthodologique du 15 novembre 2026, pp. 19–22, QI05"
            },
            {
              "id": "QI05_S1",
              "label": {
                "fr": "Source précise permettant de retrouver le résultat",
                "en": "Specific source enabling the result to be located",
                "es": "Fuente precisa que permite localizar el resultado",
                "pt": "Fonte precisa que permite localizar o resultado",
                "ar": "مصدر دقيق يتيح العثور على النتيجة"
              },
              "type": "single",
              "options": [
                {
                  "code": 1,
                  "label": {
                    "fr": "Présente dans la réponse",
                    "en": "Included in the response",
                    "es": "Incluida en la respuesta",
                    "pt": "Presente na resposta",
                    "ar": "واردة في الإجابة"
                  }
                },
                {
                  "code": 2,
                  "label": {
                    "fr": "Accessible par un lien ou une référence précise transmis avec la réponse",
                    "en": "Available through a specific link or reference sent with the response",
                    "es": "Accesible mediante un enlace o referencia precisa enviados con la respuesta",
                    "pt": "Acessível por uma ligação ou referência precisa enviada com a resposta",
                    "ar": "متاحة عبر رابط أو مرجع دقيق أرفق بالإجابة"
                  }
                },
                {
                  "code": 0,
                  "label": {
                    "fr": "Absente",
                    "en": "Absent",
                    "es": "Ausente",
                    "pt": "Ausente",
                    "ar": "غير موجودة"
                  }
                },
                {
                  "code": 98,
                  "label": {
                    "fr": "Ne sait pas",
                    "en": "Do not know",
                    "es": "No sabe",
                    "pt": "Não sabe",
                    "ar": "لا أعرف"
                  }
                },
                {
                  "code": 99,
                  "label": {
                    "fr": "Non applicable",
                    "en": "Not applicable",
                    "es": "No aplicable",
                    "pt": "Não aplicável",
                    "ar": "لا ينطبق"
                  }
                }
              ],
              "source": "Rapport méthodologique du 15 novembre 2026, pp. 19–22, QI05",
              "gate": {
                "field": "QI01_Participation",
                "op": "in",
                "values": [
                  1
                ]
              }
            },
            {
              "id": "QI05_S2",
              "label": {
                "fr": "Date de publication ou version de la source",
                "en": "Publication date or source version",
                "es": "Fecha de publicación o versión de la fuente",
                "pt": "Data de publicação ou versão da fonte",
                "ar": "تاريخ النشر أو نسخة المصدر"
              },
              "type": "single",
              "options": [
                {
                  "code": 1,
                  "label": {
                    "fr": "Présente dans la réponse",
                    "en": "Included in the response",
                    "es": "Incluida en la respuesta",
                    "pt": "Presente na resposta",
                    "ar": "واردة في الإجابة"
                  }
                },
                {
                  "code": 2,
                  "label": {
                    "fr": "Accessible par un lien ou une référence précise transmis avec la réponse",
                    "en": "Available through a specific link or reference sent with the response",
                    "es": "Accesible mediante un enlace o referencia precisa enviados con la respuesta",
                    "pt": "Acessível por uma ligação ou referência precisa enviada com a resposta",
                    "ar": "متاحة عبر رابط أو مرجع دقيق أرفق بالإجابة"
                  }
                },
                {
                  "code": 0,
                  "label": {
                    "fr": "Absente",
                    "en": "Absent",
                    "es": "Ausente",
                    "pt": "Ausente",
                    "ar": "غير موجودة"
                  }
                },
                {
                  "code": 98,
                  "label": {
                    "fr": "Ne sait pas",
                    "en": "Do not know",
                    "es": "No sabe",
                    "pt": "Não sabe",
                    "ar": "لا أعرف"
                  }
                },
                {
                  "code": 99,
                  "label": {
                    "fr": "Non applicable",
                    "en": "Not applicable",
                    "es": "No aplicable",
                    "pt": "Não aplicável",
                    "ar": "لا ينطبق"
                  }
                }
              ],
              "source": "Rapport méthodologique du 15 novembre 2026, pp. 19–22, QI05",
              "gate": {
                "field": "QI01_Participation",
                "op": "in",
                "values": [
                  1
                ]
              }
            },
            {
              "id": "QI05_S3",
              "label": {
                "fr": "Période à laquelle se rapporte la valeur",
                "en": "Period to which the value relates",
                "es": "Período al que se refiere el valor",
                "pt": "Período a que se refere o valor",
                "ar": "الفترة التي تتعلق بها القيمة"
              },
              "type": "single",
              "options": [
                {
                  "code": 1,
                  "label": {
                    "fr": "Présente dans la réponse",
                    "en": "Included in the response",
                    "es": "Incluida en la respuesta",
                    "pt": "Presente na resposta",
                    "ar": "واردة في الإجابة"
                  }
                },
                {
                  "code": 2,
                  "label": {
                    "fr": "Accessible par un lien ou une référence précise transmis avec la réponse",
                    "en": "Available through a specific link or reference sent with the response",
                    "es": "Accesible mediante un enlace o referencia precisa enviados con la respuesta",
                    "pt": "Acessível por uma ligação ou referência precisa enviada com a resposta",
                    "ar": "متاحة عبر رابط أو مرجع دقيق أرفق بالإجابة"
                  }
                },
                {
                  "code": 0,
                  "label": {
                    "fr": "Absente",
                    "en": "Absent",
                    "es": "Ausente",
                    "pt": "Ausente",
                    "ar": "غير موجودة"
                  }
                },
                {
                  "code": 98,
                  "label": {
                    "fr": "Ne sait pas",
                    "en": "Do not know",
                    "es": "No sabe",
                    "pt": "Não sabe",
                    "ar": "لا أعرف"
                  }
                },
                {
                  "code": 99,
                  "label": {
                    "fr": "Non applicable",
                    "en": "Not applicable",
                    "es": "No aplicable",
                    "pt": "Não aplicável",
                    "ar": "لا ينطبق"
                  }
                }
              ],
              "source": "Rapport méthodologique du 15 novembre 2026, pp. 19–22, QI05",
              "gate": {
                "field": "QI01_Participation",
                "op": "in",
                "values": [
                  1
                ]
              }
            },
            {
              "id": "QI05_S4",
              "label": {
                "fr": "Définition de l’indicateur",
                "en": "Definition of the indicator",
                "es": "Definición del indicador",
                "pt": "Definição do indicador",
                "ar": "تعريف المؤشر"
              },
              "type": "single",
              "options": [
                {
                  "code": 1,
                  "label": {
                    "fr": "Présente dans la réponse",
                    "en": "Included in the response",
                    "es": "Incluida en la respuesta",
                    "pt": "Presente na resposta",
                    "ar": "واردة في الإجابة"
                  }
                },
                {
                  "code": 2,
                  "label": {
                    "fr": "Accessible par un lien ou une référence précise transmis avec la réponse",
                    "en": "Available through a specific link or reference sent with the response",
                    "es": "Accesible mediante un enlace o referencia precisa enviados con la respuesta",
                    "pt": "Acessível por uma ligação ou referência precisa enviada com a resposta",
                    "ar": "متاحة عبر رابط أو مرجع دقيق أرفق بالإجابة"
                  }
                },
                {
                  "code": 0,
                  "label": {
                    "fr": "Absente",
                    "en": "Absent",
                    "es": "Ausente",
                    "pt": "Ausente",
                    "ar": "غير موجودة"
                  }
                },
                {
                  "code": 98,
                  "label": {
                    "fr": "Ne sait pas",
                    "en": "Do not know",
                    "es": "No sabe",
                    "pt": "Não sabe",
                    "ar": "لا أعرف"
                  }
                },
                {
                  "code": 99,
                  "label": {
                    "fr": "Non applicable",
                    "en": "Not applicable",
                    "es": "No aplicable",
                    "pt": "Não aplicável",
                    "ar": "لا ينطبق"
                  }
                }
              ],
              "source": "Rapport méthodologique du 15 novembre 2026, pp. 19–22, QI05",
              "gate": {
                "field": "QI01_Participation",
                "op": "in",
                "values": [
                  1
                ]
              }
            },
            {
              "id": "QI05_S5",
              "label": {
                "fr": "Unité et base de calcul, notamment dénominateur d’un taux",
                "en": "Unit and calculation basis, particularly the denominator of a rate",
                "es": "Unidad y base de cálculo, en particular el denominador de una tasa",
                "pt": "Unidade e base de cálculo, nomeadamente o denominador de uma taxa",
                "ar": "الوحدة وأساس الحساب، ولا سيما مقام المعدل"
              },
              "type": "single",
              "options": [
                {
                  "code": 1,
                  "label": {
                    "fr": "Présente dans la réponse",
                    "en": "Included in the response",
                    "es": "Incluida en la respuesta",
                    "pt": "Presente na resposta",
                    "ar": "واردة في الإجابة"
                  }
                },
                {
                  "code": 2,
                  "label": {
                    "fr": "Accessible par un lien ou une référence précise transmis avec la réponse",
                    "en": "Available through a specific link or reference sent with the response",
                    "es": "Accesible mediante un enlace o referencia precisa enviados con la respuesta",
                    "pt": "Acessível por uma ligação ou referência precisa enviada com a resposta",
                    "ar": "متاحة عبر رابط أو مرجع دقيق أرفق بالإجابة"
                  }
                },
                {
                  "code": 0,
                  "label": {
                    "fr": "Absente",
                    "en": "Absent",
                    "es": "Ausente",
                    "pt": "Ausente",
                    "ar": "غير موجودة"
                  }
                },
                {
                  "code": 98,
                  "label": {
                    "fr": "Ne sait pas",
                    "en": "Do not know",
                    "es": "No sabe",
                    "pt": "Não sabe",
                    "ar": "لا أعرف"
                  }
                },
                {
                  "code": 99,
                  "label": {
                    "fr": "Non applicable",
                    "en": "Not applicable",
                    "es": "No aplicable",
                    "pt": "Não aplicável",
                    "ar": "لا ينطبق"
                  }
                }
              ],
              "source": "Rapport méthodologique du 15 novembre 2026, pp. 19–22, QI05",
              "gate": {
                "field": "QI01_Participation",
                "op": "in",
                "values": [
                  1
                ]
              }
            },
            {
              "id": "QI05_S6",
              "label": {
                "fr": "Population et territoire couverts",
                "en": "Population and geographical coverage",
                "es": "Población y territorio cubiertos",
                "pt": "População e território abrangidos",
                "ar": "السكان والنطاق الجغرافي المشمولان"
              },
              "type": "single",
              "options": [
                {
                  "code": 1,
                  "label": {
                    "fr": "Présente dans la réponse",
                    "en": "Included in the response",
                    "es": "Incluida en la respuesta",
                    "pt": "Presente na resposta",
                    "ar": "واردة في الإجابة"
                  }
                },
                {
                  "code": 2,
                  "label": {
                    "fr": "Accessible par un lien ou une référence précise transmis avec la réponse",
                    "en": "Available through a specific link or reference sent with the response",
                    "es": "Accesible mediante un enlace o referencia precisa enviados con la respuesta",
                    "pt": "Acessível por uma ligação ou referência precisa enviada com a resposta",
                    "ar": "متاحة عبر رابط أو مرجع دقيق أرفق بالإجابة"
                  }
                },
                {
                  "code": 0,
                  "label": {
                    "fr": "Absente",
                    "en": "Absent",
                    "es": "Ausente",
                    "pt": "Ausente",
                    "ar": "غير موجودة"
                  }
                },
                {
                  "code": 98,
                  "label": {
                    "fr": "Ne sait pas",
                    "en": "Do not know",
                    "es": "No sabe",
                    "pt": "Não sabe",
                    "ar": "لا أعرف"
                  }
                },
                {
                  "code": 99,
                  "label": {
                    "fr": "Non applicable",
                    "en": "Not applicable",
                    "es": "No aplicable",
                    "pt": "Não aplicável",
                    "ar": "لا ينطبق"
                  }
                }
              ],
              "source": "Rapport méthodologique du 15 novembre 2026, pp. 19–22, QI05",
              "gate": {
                "field": "QI01_Participation",
                "op": "in",
                "values": [
                  1
                ]
              }
            },
            {
              "id": "QI05_S7",
              "label": {
                "fr": "Limites utiles à l’interprétation ou à la comparaison",
                "en": "Limitations relevant to interpretation or comparison",
                "es": "Limitaciones pertinentes para interpretar o comparar",
                "pt": "Limitações relevantes para interpretar ou comparar",
                "ar": "القيود المفيدة للتفسير أو المقارنة"
              },
              "type": "single",
              "options": [
                {
                  "code": 1,
                  "label": {
                    "fr": "Présente dans la réponse",
                    "en": "Included in the response",
                    "es": "Incluida en la respuesta",
                    "pt": "Presente na resposta",
                    "ar": "واردة في الإجابة"
                  }
                },
                {
                  "code": 2,
                  "label": {
                    "fr": "Accessible par un lien ou une référence précise transmis avec la réponse",
                    "en": "Available through a specific link or reference sent with the response",
                    "es": "Accesible mediante un enlace o referencia precisa enviados con la respuesta",
                    "pt": "Acessível por uma ligação ou referência precisa enviada com a resposta",
                    "ar": "متاحة عبر رابط أو مرجع دقيق أرفق بالإجابة"
                  }
                },
                {
                  "code": 0,
                  "label": {
                    "fr": "Absente",
                    "en": "Absent",
                    "es": "Ausente",
                    "pt": "Ausente",
                    "ar": "غير موجودة"
                  }
                },
                {
                  "code": 98,
                  "label": {
                    "fr": "Ne sait pas",
                    "en": "Do not know",
                    "es": "No sabe",
                    "pt": "Não sabe",
                    "ar": "لا أعرف"
                  }
                },
                {
                  "code": 99,
                  "label": {
                    "fr": "Non applicable",
                    "en": "Not applicable",
                    "es": "No aplicable",
                    "pt": "Não aplicável",
                    "ar": "لا ينطبق"
                  }
                }
              ],
              "source": "Rapport méthodologique du 15 novembre 2026, pp. 19–22, QI05",
              "gate": {
                "field": "QI01_Participation",
                "op": "in",
                "values": [
                  1
                ]
              }
            },
            {
              "id": "QI05_S8",
              "label": {
                "fr": "Statut provisoire ou révisé, lorsqu’il s’applique",
                "en": "Provisional or revised status, where applicable",
                "es": "Carácter provisional o revisado, cuando proceda",
                "pt": "Caráter provisório ou revisto, quando aplicável",
                "ar": "صفة مؤقتة أو منقحة، عند الاقتضاء"
              },
              "type": "single",
              "options": [
                {
                  "code": 1,
                  "label": {
                    "fr": "Présente dans la réponse",
                    "en": "Included in the response",
                    "es": "Incluida en la respuesta",
                    "pt": "Presente na resposta",
                    "ar": "واردة في الإجابة"
                  }
                },
                {
                  "code": 2,
                  "label": {
                    "fr": "Accessible par un lien ou une référence précise transmis avec la réponse",
                    "en": "Available through a specific link or reference sent with the response",
                    "es": "Accesible mediante un enlace o referencia precisa enviados con la respuesta",
                    "pt": "Acessível por uma ligação ou referência precisa enviada com a resposta",
                    "ar": "متاحة عبر رابط أو مرجع دقيق أرفق بالإجابة"
                  }
                },
                {
                  "code": 0,
                  "label": {
                    "fr": "Absente",
                    "en": "Absent",
                    "es": "Ausente",
                    "pt": "Ausente",
                    "ar": "غير موجودة"
                  }
                },
                {
                  "code": 98,
                  "label": {
                    "fr": "Ne sait pas",
                    "en": "Do not know",
                    "es": "No sabe",
                    "pt": "Não sabe",
                    "ar": "لا أعرف"
                  }
                },
                {
                  "code": 99,
                  "label": {
                    "fr": "Non applicable",
                    "en": "Not applicable",
                    "es": "No aplicable",
                    "pt": "Não aplicável",
                    "ar": "لا ينطبق"
                  }
                }
              ],
              "source": "Rapport méthodologique du 15 novembre 2026, pp. 19–22, QI05",
              "gate": {
                "field": "QI01_Participation",
                "op": "in",
                "values": [
                  1
                ]
              }
            }
          ],
          "note": {
            "fr": "Considérer le premier chiffre présenté dans la dernière réponse transmise, et non le mieux documenté. Indiquer les informations qui l’accompagnaient effectivement. Une simple mention du nom d’un organisme ne suffit pas pour le code 2. Ne pas recopier une valeur non publique.",
            "en": "Consider the first figure presented in the most recent response, not the best-documented figure. Indicate the information that actually accompanied it. Merely naming an organisation does not qualify for code 2. Do not copy a non-public value.",
            "es": "Considere la primera cifra de la última respuesta enviada, no la mejor documentada. Indique qué información la acompañaba realmente. Mencionar solo el nombre de una entidad no basta para el código 2. No copie un valor no público.",
            "pt": "Considere o primeiro número da última resposta enviada, não o mais bem documentado. Indique as informações que o acompanhavam efetivamente. Mencionar apenas o nome de uma entidade não basta para o código 2. Não copie um valor não público.",
            "ar": "اعتمد أول رقم ورد في آخر إجابة مرسلة، لا الرقم الأفضل توثيقا. حدد المعلومات التي رافقته فعليا. لا يكفي ذكر اسم جهة فقط لاختيار الرمز 2. لا تنقل قيمة غير متاحة للعموم."
          },
          "gate": {
            "all": [
              {
                "field": "QI02a",
                "op": "not_in",
                "values": [
                  0
                ]
              },
              {
                "field": "QI03_Situation",
                "op": "in",
                "values": [
                  1
                ]
              }
            ]
          },
          "theme": "Sources et métadonnées",
          "source": "Rapport méthodologique du 15 novembre 2026, pp. 19–22, QI05"
        },
        {
          "id": "QI06",
          "title": {
            "fr": "Vérification et validation effectivement réalisées",
            "en": "Checking and validation actually performed",
            "es": "Comprobación y validación efectivamente realizadas",
            "pt": "Verificação e validação efetivamente realizadas",
            "ar": "المراجعة والتحقق المنجزان فعليا"
          },
          "fields": [
            {
              "id": "QI06a",
              "label": {
                "fr": "Qui a vérifié le contenu statistique avant son envoi ? Plusieurs réponses possibles.",
                "en": "Who checked the statistical content before it was sent? Multiple answers allowed.",
                "es": "¿Quién comprobó el contenido estadístico antes de enviarlo? Se admiten varias respuestas.",
                "pt": "Quem verificou o conteúdo estatístico antes do envio? São admitidas várias respostas.",
                "ar": "من راجع المحتوى الإحصائي قبل إرساله؟ يمكن اختيار عدة إجابات."
              },
              "type": "multi",
              "options": [
                {
                  "code": 1,
                  "label": {
                    "fr": "L’agent qui l’a préparé",
                    "en": "The staff member who prepared it",
                    "es": "El agente que lo preparó",
                    "pt": "O agente que o preparou",
                    "ar": "الموظف الذي أعده"
                  }
                },
                {
                  "code": 2,
                  "label": {
                    "fr": "Un autre agent producteur des données",
                    "en": "Another staff member producing the data",
                    "es": "Otro agente productor de los datos",
                    "pt": "Outro agente produtor dos dados",
                    "ar": "موظف آخر منتج للبيانات"
                  }
                },
                {
                  "code": 3,
                  "label": {
                    "fr": "Un responsable technique ou de qualité",
                    "en": "A technical or quality manager",
                    "es": "Un responsable técnico o de calidad",
                    "pt": "Um responsável técnico ou de qualidade",
                    "ar": "مسؤول فني أو مسؤول جودة"
                  }
                },
                {
                  "code": 4,
                  "label": {
                    "fr": "Le service de communication",
                    "en": "The communication service",
                    "es": "El servicio de comunicación",
                    "pt": "O serviço de comunicação",
                    "ar": "إدارة الاتصال"
                  }
                },
                {
                  "code": 5,
                  "label": {
                    "fr": "La direction ou son délégataire",
                    "en": "Management or its delegate",
                    "es": "La dirección o su delegado",
                    "pt": "A direção ou o seu delegado",
                    "ar": "الإدارة العليا أو من تفوضه"
                  }
                },
                {
                  "code": 0,
                  "label": {
                    "fr": "Aucun contrôle connu",
                    "en": "No known check",
                    "es": "Ningún control conocido",
                    "pt": "Nenhum controlo conhecido",
                    "ar": "لا أعرف أي مراجعة"
                  }
                },
                {
                  "code": 97,
                  "label": {
                    "fr": "Autre fonction",
                    "en": "Other function",
                    "es": "Otra función",
                    "pt": "Outra função",
                    "ar": "وظيفة أخرى"
                  }
                },
                {
                  "code": 98,
                  "label": {
                    "fr": "Ne sait pas",
                    "en": "Do not know",
                    "es": "No sabe",
                    "pt": "Não sabe",
                    "ar": "لا أعرف"
                  }
                }
              ],
              "exclusive": [
                0,
                98
              ],
              "source": "Rapport méthodologique du 15 novembre 2026, pp. 19–22, QI06",
              "gate": {
                "field": "QI01_Participation",
                "op": "in",
                "values": [
                  1
                ]
              }
            },
            {
              "id": "QI06a_Autre",
              "label": {
                "fr": "Autre : précisez",
                "en": "Other: please specify",
                "es": "Otra : especifique",
                "pt": "Outra : especifique",
                "ar": "أخرى : يرجى التحديد"
              },
              "type": "text",
              "options": [],
              "max_chars": 500,
              "gate": {
                "all": [
                  {
                    "field": "QI01_Participation",
                    "op": "in",
                    "values": [
                      1
                    ]
                  },
                  {
                    "field": "QI06a",
                    "op": "contains",
                    "values": [
                      97
                    ]
                  }
                ]
              },
              "source": "Rapport méthodologique du 15 novembre 2026, pp. 19–22, QI06"
            },
            {
              "id": "QI06b",
              "label": {
                "fr": "Quelle trace de la validation statistique finale existe pour cet envoi ?",
                "en": "What record exists of the final statistical validation for this submission?",
                "es": "¿Qué constancia existe de la validación estadística final de este envío?",
                "pt": "Que registo existe da validação estatística final deste envio?",
                "ar": "ما الإثبات الموجود للتحقق الإحصائي النهائي لهذا الإرسال؟"
              },
              "type": "single",
              "options": [
                {
                  "code": 1,
                  "label": {
                    "fr": "Trace écrite, courriel, visa ou journal de validation",
                    "en": "Written record, email, sign-off or validation log",
                    "es": "Constancia escrita, correo, visado o registro de validación",
                    "pt": "Registo escrito, correio, visto ou diário de validação",
                    "ar": "إثبات كتابي أو بريد أو تأشيرة اعتماد أو سجل تحقق"
                  }
                },
                {
                  "code": 2,
                  "label": {
                    "fr": "Accord oral seulement",
                    "en": "Oral agreement only",
                    "es": "Solo acuerdo oral",
                    "pt": "Apenas acordo oral",
                    "ar": "موافقة شفهية فقط"
                  }
                },
                {
                  "code": 0,
                  "label": {
                    "fr": "Aucune validation distincte connue",
                    "en": "No separate validation known",
                    "es": "Ninguna validación independiente conocida",
                    "pt": "Nenhuma validação distinta conhecida",
                    "ar": "لا أعرف تحققا منفصلا"
                  }
                },
                {
                  "code": 98,
                  "label": {
                    "fr": "Ne sait pas",
                    "en": "Do not know",
                    "es": "No sabe",
                    "pt": "Não sabe",
                    "ar": "لا أعرف"
                  }
                }
              ],
              "source": "Rapport méthodologique du 15 novembre 2026, pp. 19–22, QI06",
              "gate": {
                "field": "QI01_Participation",
                "op": "in",
                "values": [
                  1
                ]
              }
            }
          ],
          "note": {
            "fr": "Il s’agit du contrôle du contenu statistique, pas de l’approbation politique du discours.",
            "en": "This concerns checking statistical content, not political approval of the speech.",
            "es": "Se trata del control del contenido estadístico, no de la aprobación política del discurso.",
            "pt": "Trata-se do controlo do conteúdo estatístico, não da aprovação política do discurso.",
            "ar": "المقصود مراجعة المحتوى الإحصائي، وليس الموافقة السياسية على الخطاب."
          },
          "gate": {
            "all": [
              {
                "field": "QI02a",
                "op": "not_in",
                "values": [
                  0
                ]
              },
              {
                "field": "QI03_Situation",
                "op": "in",
                "values": [
                  1
                ]
              }
            ]
          },
          "theme": "Vérification",
          "source": "Rapport méthodologique du 15 novembre 2026, pp. 19–22, QI06"
        },
        {
          "id": "QI07",
          "title": {
            "fr": "Format transmis et vérification du passage de discours",
            "en": "Format provided and checking of the speech passage",
            "es": "Formato facilitado y comprobación del pasaje del discurso",
            "pt": "Formato fornecido e verificação do trecho do discurso",
            "ar": "صيغة الدعم المقدم ومراجعة مقطع الخطاب"
          },
          "fields": [
            {
              "id": "QI07a",
              "label": {
                "fr": "Sous quelle forme l’appui a-t-il été transmis ? Jusqu’à trois réponses.",
                "en": "In what form was the support provided? Up to three answers.",
                "es": "¿En qué formato se proporcionó el apoyo? Hasta tres respuestas.",
                "pt": "Em que formato foi fornecido o apoio? Até três respostas.",
                "ar": "بأي صيغة قدم الدعم؟ ثلاث إجابات كحد أقصى."
              },
              "type": "multi",
              "options": [
                {
                  "code": 1,
                  "label": {
                    "fr": "Tableau ou liste de chiffres",
                    "en": "Table or list of figures",
                    "es": "Tabla o lista de cifras",
                    "pt": "Tabela ou lista de números",
                    "ar": "جدول أو قائمة أرقام"
                  }
                },
                {
                  "code": 2,
                  "label": {
                    "fr": "Fiche accompagnée d’explications",
                    "en": "Fact sheet with explanations",
                    "es": "Ficha con explicaciones",
                    "pt": "Ficha com explicações",
                    "ar": "بطاقة مع توضيحات"
                  }
                },
                {
                  "code": 3,
                  "label": {
                    "fr": "Proposition de phrases ou de paragraphes",
                    "en": "Suggested sentences or paragraphs",
                    "es": "Propuesta de frases o párrafos",
                    "pt": "Proposta de frases ou parágrafos",
                    "ar": "اقتراح جمل أو فقرات"
                  }
                },
                {
                  "code": 4,
                  "label": {
                    "fr": "Graphique ou illustration",
                    "en": "Chart or illustration",
                    "es": "Gráfico o ilustración",
                    "pt": "Gráfico ou ilustração",
                    "ar": "رسم بياني أو توضيحي"
                  }
                },
                {
                  "code": 5,
                  "label": {
                    "fr": "Explication orale uniquement",
                    "en": "Oral explanation only",
                    "es": "Solo explicación oral",
                    "pt": "Apenas explicação oral",
                    "ar": "شرح شفهي فقط"
                  }
                },
                {
                  "code": 97,
                  "label": {
                    "fr": "Autre",
                    "en": "Other",
                    "es": "Otra",
                    "pt": "Outra",
                    "ar": "أخرى"
                  }
                },
                {
                  "code": 98,
                  "label": {
                    "fr": "Ne sait pas",
                    "en": "Do not know",
                    "es": "No sabe",
                    "pt": "Não sabe",
                    "ar": "لا أعرف"
                  }
                }
              ],
              "max_choices": 3,
              "exclusive": [
                98
              ],
              "source": "Rapport méthodologique du 15 novembre 2026, pp. 19–22, QI07",
              "gate": {
                "field": "QI01_Participation",
                "op": "in",
                "values": [
                  1
                ]
              }
            },
            {
              "id": "QI07a_Autre",
              "label": {
                "fr": "Autre : précisez",
                "en": "Other: please specify",
                "es": "Otra : especifique",
                "pt": "Outra : especifique",
                "ar": "أخرى : يرجى التحديد"
              },
              "type": "text",
              "options": [],
              "max_chars": 500,
              "gate": {
                "all": [
                  {
                    "field": "QI01_Participation",
                    "op": "in",
                    "values": [
                      1
                    ]
                  },
                  {
                    "field": "QI07a",
                    "op": "contains",
                    "values": [
                      97
                    ]
                  }
                ]
              },
              "source": "Rapport méthodologique du 15 novembre 2026, pp. 19–22, QI07"
            },
            {
              "id": "QI07b",
              "label": {
                "fr": "L’INS a-t-il ensuite pu vérifier les chiffres dans le passage de discours préparé avant sa diffusion ?",
                "en": "Was the NSO subsequently able to check the figures in the prepared speech passage before dissemination?",
                "es": "¿Pudo la ONE comprobar posteriormente las cifras en el pasaje preparado antes de su difusión?",
                "pt": "O INE pôde posteriormente verificar os números no trecho preparado antes da divulgação?",
                "ar": "هل تمكن الجهاز الوطني للإحصاء لاحقا من مراجعة الأرقام في مقطع الخطاب المعد قبل نشره؟"
              },
              "type": "single",
              "options": [
                {
                  "code": 1,
                  "label": {
                    "fr": "Oui",
                    "en": "Yes",
                    "es": "Sí",
                    "pt": "Sim",
                    "ar": "نعم"
                  }
                },
                {
                  "code": 2,
                  "label": {
                    "fr": "Non, le passage n’a pas été reçu",
                    "en": "No, the passage was not received",
                    "es": "No, no se recibió el pasaje",
                    "pt": "Não, o trecho não foi recebido",
                    "ar": "لا، لم يستلم المقطع"
                  }
                },
                {
                  "code": 3,
                  "label": {
                    "fr": "Non, le passage a été reçu mais n’a pas été vérifié",
                    "en": "No, the passage was received but not checked",
                    "es": "No, se recibió el pasaje pero no se comprobó",
                    "pt": "Não, o trecho foi recebido mas não foi verificado",
                    "ar": "لا، استلم المقطع لكنه لم يراجع"
                  }
                },
                {
                  "code": 4,
                  "label": {
                    "fr": "Le discours n’a pas été diffusé ou cette étape n’a pas eu lieu",
                    "en": "The speech was not delivered or this stage did not occur",
                    "es": "El discurso no se difundió o esta etapa no tuvo lugar",
                    "pt": "O discurso não foi divulgado ou esta etapa não ocorreu",
                    "ar": "لم ينشر الخطاب أو لم تتم هذه المرحلة"
                  }
                },
                {
                  "code": 98,
                  "label": {
                    "fr": "Ne sait pas",
                    "en": "Do not know",
                    "es": "No sabe",
                    "pt": "Não sabe",
                    "ar": "لا أعرف"
                  }
                }
              ],
              "source": "Rapport méthodologique du 15 novembre 2026, pp. 19–22, QI07",
              "gate": {
                "field": "QI01_Participation",
                "op": "in",
                "values": [
                  1
                ]
              }
            }
          ],
          "note": {
            "fr": "",
            "en": "",
            "es": "",
            "pt": "",
            "ar": ""
          },
          "gate": {
            "all": [
              {
                "field": "QI02a",
                "op": "not_in",
                "values": [
                  0
                ]
              },
              {
                "field": "QI03_Situation",
                "op": "in",
                "values": [
                  1
                ]
              }
            ]
          },
          "theme": "Messages et publics",
          "source": "Rapport méthodologique du 15 novembre 2026, pp. 19–22, QI07"
        },
        {
          "id": "QI08",
          "title": {
            "fr": "Correction ou actualisation après transmission",
            "en": "Correction or updating after transmission",
            "es": "Corrección o actualización después del envío",
            "pt": "Correção ou atualização após o envio",
            "ar": "التصحيح أو التحديث بعد الإرسال"
          },
          "fields": [
            {
              "id": "QI08_Situation",
              "label": {
                "fr": "Pendant la période de référence, avez-vous connaissance d’un appui déjà transmis qu’il a fallu corriger ou actualiser ?",
                "en": "During the reference period, do you know of support already provided that had to be corrected or updated?",
                "es": "Durante el período de referencia, ¿conoce algún apoyo ya enviado que debiera corregirse o actualizarse?",
                "pt": "Durante o período de referência, conhece algum apoio já enviado que tenha exigido correção ou atualização?",
                "ar": "هل تعرف دعما سبق إرساله واستلزم التصحيح أو التحديث خلال الفترة المرجعية؟"
              },
              "type": "single",
              "options": [
                {
                  "code": 1,
                  "label": {
                    "fr": "Oui, pour corriger une erreur",
                    "en": "Yes, to correct an error",
                    "es": "Sí, para corregir un error",
                    "pt": "Sim, para corrigir um erro",
                    "ar": "نعم، لتصحيح خطأ"
                  }
                },
                {
                  "code": 2,
                  "label": {
                    "fr": "Oui, pour actualiser une donnée",
                    "en": "Yes, to update data",
                    "es": "Sí, para actualizar un dato",
                    "pt": "Sim, para atualizar um dado",
                    "ar": "نعم، لتحديث بيانات"
                  }
                },
                {
                  "code": 3,
                  "label": {
                    "fr": "Les deux situations ont été rencontrées",
                    "en": "Both situations occurred",
                    "es": "Se produjeron ambas situaciones",
                    "pt": "Ocorreram ambas as situações",
                    "ar": "حدثت الحالتان"
                  }
                },
                {
                  "code": 0,
                  "label": {
                    "fr": "Aucun épisode connu",
                    "en": "No known instance",
                    "es": "Ningún episodio conocido",
                    "pt": "Nenhum episódio conhecido",
                    "ar": "لا أعرف أي حالة"
                  }
                },
                {
                  "code": 98,
                  "label": {
                    "fr": "Ne sait pas",
                    "en": "Do not know",
                    "es": "No sabe",
                    "pt": "Não sabe",
                    "ar": "لا أعرف"
                  }
                },
                {
                  "code": 99,
                  "label": {
                    "fr": "Non applicable, aucun appui concerné",
                    "en": "Not applicable, no relevant support",
                    "es": "No aplicable, ningún apoyo pertinente",
                    "pt": "Não aplicável, nenhum apoio em causa",
                    "ar": "لا ينطبق، لا يوجد دعم معني"
                  }
                }
              ],
              "source": "Rapport méthodologique du 15 novembre 2026, pp. 19–22, QI08",
              "gate": {
                "field": "QI01_Participation",
                "op": "in",
                "values": [
                  1
                ]
              }
            },
            {
              "id": "QI08_Actions",
              "label": {
                "fr": "Pour le cas le plus récent, quelles actions ont été réalisées ? Plusieurs réponses possibles.",
                "en": "For the most recent case, what actions were taken? Multiple answers allowed.",
                "es": "En el caso más reciente, ¿qué acciones se realizaron? Se admiten varias respuestas.",
                "pt": "No caso mais recente, que ações foram realizadas? São admitidas várias respostas.",
                "ar": "ما الإجراءات المتخذة في أحدث حالة؟ يمكن اختيار عدة إجابات."
              },
              "type": "multi",
              "options": [
                {
                  "code": 1,
                  "label": {
                    "fr": "Alerte de l’équipe préparant le discours",
                    "en": "Alerting the speech-preparation team",
                    "es": "Aviso al equipo que prepara el discurso",
                    "pt": "Alerta à equipa que prepara o discurso",
                    "ar": "تنبيه فريق إعداد الخطاب"
                  }
                },
                {
                  "code": 2,
                  "label": {
                    "fr": "Envoi d’une version corrigée avec sa source",
                    "en": "Sending a corrected version with its source",
                    "es": "Envío de una versión corregida con su fuente",
                    "pt": "Envio de uma versão corrigida com a fonte",
                    "ar": "إرسال نسخة مصححة مع مصدرها"
                  }
                },
                {
                  "code": 3,
                  "label": {
                    "fr": "Conservation des versions et du motif de changement",
                    "en": "Keeping versions and the reason for the change",
                    "es": "Conservación de las versiones y del motivo del cambio",
                    "pt": "Conservação das versões e do motivo da alteração",
                    "ar": "حفظ النسخ وسبب التغيير"
                  }
                },
                {
                  "code": 4,
                  "label": {
                    "fr": "Demande de correction d’un passage déjà diffusé",
                    "en": "Requesting correction of a passage already disseminated",
                    "es": "Solicitud de corrección de un pasaje ya difundido",
                    "pt": "Pedido de correção de um trecho já divulgado",
                    "ar": "طلب تصحيح مقطع سبق نشره"
                  }
                },
                {
                  "code": 5,
                  "label": {
                    "fr": "Vérification de la prise en compte de la correction",
                    "en": "Checking that the correction was incorporated",
                    "es": "Verificación de que se incorporó la corrección",
                    "pt": "Verificação de que a correção foi incorporada",
                    "ar": "التحقق من أخذ التصحيح في الاعتبار"
                  }
                },
                {
                  "code": 0,
                  "label": {
                    "fr": "Aucune action connue",
                    "en": "No known action",
                    "es": "Ninguna acción conocida",
                    "pt": "Nenhuma ação conhecida",
                    "ar": "لا أعرف أي إجراء"
                  }
                },
                {
                  "code": 97,
                  "label": {
                    "fr": "Autre",
                    "en": "Other",
                    "es": "Otra",
                    "pt": "Outra",
                    "ar": "أخرى"
                  }
                },
                {
                  "code": 98,
                  "label": {
                    "fr": "Ne sait pas",
                    "en": "Do not know",
                    "es": "No sabe",
                    "pt": "Não sabe",
                    "ar": "لا أعرف"
                  }
                }
              ],
              "exclusive": [
                0,
                98
              ],
              "gate": {
                "all": [
                  {
                    "field": "QI01_Participation",
                    "op": "in",
                    "values": [
                      1
                    ]
                  },
                  {
                    "field": "QI08_Situation",
                    "op": "in",
                    "values": [
                      1,
                      2,
                      3
                    ]
                  }
                ]
              },
              "source": "Rapport méthodologique du 15 novembre 2026, pp. 19–22, QI08"
            },
            {
              "id": "QI08_Actions_Autre",
              "label": {
                "fr": "Autre : précisez",
                "en": "Other: please specify",
                "es": "Otra : especifique",
                "pt": "Outra : especifique",
                "ar": "أخرى : يرجى التحديد"
              },
              "type": "text",
              "options": [],
              "max_chars": 500,
              "gate": {
                "all": [
                  {
                    "field": "QI01_Participation",
                    "op": "in",
                    "values": [
                      1
                    ]
                  },
                  {
                    "field": "QI08_Actions",
                    "op": "contains",
                    "values": [
                      97
                    ]
                  }
                ]
              },
              "source": "Rapport méthodologique du 15 novembre 2026, pp. 19–22, QI08"
            }
          ],
          "note": {
            "fr": "Un appui initialement fourni avant la période peut être pris en compte si la correction intervient pendant celle-ci. Cette question reste accessible même si aucune nouvelle demande n’a été reçue.",
            "en": "Support originally provided before the period may be included if the correction occurred during the period. This question remains available even if no new request was received.",
            "es": "Puede incluirse un apoyo enviado antes del período si la corrección se produjo durante este. Esta pregunta sigue disponible aunque no se recibieran solicitudes nuevas.",
            "pt": "Pode incluir-se um apoio enviado antes do período se a correção ocorreu durante o mesmo. Esta pergunta mantém-se disponível mesmo sem novos pedidos.",
            "ar": "يمكن إدراج دعم قدم قبل الفترة إذا جرى تصحيحه خلالها. يبقى هذا السؤال متاحا حتى عند عدم استلام طلب جديد."
          },
          "gate": null,
          "theme": "Actualisation et correction",
          "source": "Rapport méthodologique du 15 novembre 2026, pp. 19–22, QI08"
        },
        {
          "id": "QI09",
          "title": {
            "fr": "Organisation effectivement en place au 31 octobre 2026",
            "en": "Arrangements actually in place on 31 October 2026",
            "es": "Dispositivos efectivamente existentes al 31 de octubre de 2026",
            "pt": "Dispositivos efetivamente existentes em 31 de outubro de 2026",
            "ar": "الترتيبات القائمة فعليا في 31 أكتوبر 2026"
          },
          "fields": [
            {
              "id": "QI09_D1",
              "label": {
                "fr": "Point d’entrée ou fonction identifiée pour recevoir les demandes",
                "en": "Identified contact point or function for receiving requests",
                "es": "Punto de contacto o función identificada para recibir solicitudes",
                "pt": "Ponto de contacto ou função identificada para receber pedidos",
                "ar": "نقطة اتصال أو وظيفة محددة لاستلام الطلبات"
              },
              "type": "single",
              "options": [
                {
                  "code": 1,
                  "label": {
                    "fr": "Existe et a été utilisé pour ces appuis pendant la période",
                    "en": "Exists and was used for this support during the period",
                    "es": "Existe y se utilizó para estos apoyos durante el período",
                    "pt": "Existe e foi utilizado para estes apoios durante o período",
                    "ar": "موجود واستخدم لهذا الدعم خلال الفترة"
                  }
                },
                {
                  "code": 2,
                  "label": {
                    "fr": "Existe mais n’a pas été utilisé pendant la période",
                    "en": "Exists but was not used during the period",
                    "es": "Existe pero no se utilizó durante el período",
                    "pt": "Existe mas não foi utilizado durante o período",
                    "ar": "موجود لكنه لم يستخدم خلال الفترة"
                  }
                },
                {
                  "code": 0,
                  "label": {
                    "fr": "N’existe pas",
                    "en": "Does not exist",
                    "es": "No existe",
                    "pt": "Não existe",
                    "ar": "غير موجود"
                  }
                },
                {
                  "code": 98,
                  "label": {
                    "fr": "Ne sait pas",
                    "en": "Do not know",
                    "es": "No sabe",
                    "pt": "Não sabe",
                    "ar": "لا أعرف"
                  }
                },
                {
                  "code": 99,
                  "label": {
                    "fr": "Non applicable",
                    "en": "Not applicable",
                    "es": "No aplicable",
                    "pt": "Não aplicável",
                    "ar": "لا ينطبق"
                  }
                }
              ],
              "source": "Rapport méthodologique du 15 novembre 2026, pp. 19–22, QI09",
              "gate": {
                "field": "QI01_Participation",
                "op": "in",
                "values": [
                  1
                ]
              }
            },
            {
              "id": "QI09_D2",
              "label": {
                "fr": "Consigne écrite de traitement, y compris des urgences",
                "en": "Written handling instructions, including for urgent requests",
                "es": "Instrucciones escritas de tratamiento, incluidas las urgencias",
                "pt": "Instruções escritas de tratamento, incluindo urgências",
                "ar": "تعليمات معالجة مكتوبة، بما فيها الحالات العاجلة"
              },
              "type": "single",
              "options": [
                {
                  "code": 1,
                  "label": {
                    "fr": "Existe et a été utilisé pour ces appuis pendant la période",
                    "en": "Exists and was used for this support during the period",
                    "es": "Existe y se utilizó para estos apoyos durante el período",
                    "pt": "Existe e foi utilizado para estes apoios durante o período",
                    "ar": "موجود واستخدم لهذا الدعم خلال الفترة"
                  }
                },
                {
                  "code": 2,
                  "label": {
                    "fr": "Existe mais n’a pas été utilisé pendant la période",
                    "en": "Exists but was not used during the period",
                    "es": "Existe pero no se utilizó durante el período",
                    "pt": "Existe mas não foi utilizado durante o período",
                    "ar": "موجود لكنه لم يستخدم خلال الفترة"
                  }
                },
                {
                  "code": 0,
                  "label": {
                    "fr": "N’existe pas",
                    "en": "Does not exist",
                    "es": "No existe",
                    "pt": "Não existe",
                    "ar": "غير موجود"
                  }
                },
                {
                  "code": 98,
                  "label": {
                    "fr": "Ne sait pas",
                    "en": "Do not know",
                    "es": "No sabe",
                    "pt": "Não sabe",
                    "ar": "لا أعرف"
                  }
                },
                {
                  "code": 99,
                  "label": {
                    "fr": "Non applicable",
                    "en": "Not applicable",
                    "es": "No aplicable",
                    "pt": "Não aplicável",
                    "ar": "لا ينطبق"
                  }
                }
              ],
              "source": "Rapport méthodologique du 15 novembre 2026, pp. 19–22, QI09",
              "gate": {
                "field": "QI01_Participation",
                "op": "in",
                "values": [
                  1
                ]
              }
            },
            {
              "id": "QI09_D3",
              "label": {
                "fr": "Registre des demandes avec échéance, responsable et état de traitement",
                "en": "Request register with deadline, responsible officer and processing status",
                "es": "Registro de solicitudes con plazo, responsable y estado de tramitación",
                "pt": "Registo de pedidos com prazo, responsável e estado de tratamento",
                "ar": "سجل للطلبات يتضمن المهلة والمسؤول وحالة المعالجة"
              },
              "type": "single",
              "options": [
                {
                  "code": 1,
                  "label": {
                    "fr": "Existe et a été utilisé pour ces appuis pendant la période",
                    "en": "Exists and was used for this support during the period",
                    "es": "Existe y se utilizó para estos apoyos durante el período",
                    "pt": "Existe e foi utilizado para estes apoios durante o período",
                    "ar": "موجود واستخدم لهذا الدعم خلال الفترة"
                  }
                },
                {
                  "code": 2,
                  "label": {
                    "fr": "Existe mais n’a pas été utilisé pendant la période",
                    "en": "Exists but was not used during the period",
                    "es": "Existe pero no se utilizó durante el período",
                    "pt": "Existe mas não foi utilizado durante o período",
                    "ar": "موجود لكنه لم يستخدم خلال الفترة"
                  }
                },
                {
                  "code": 0,
                  "label": {
                    "fr": "N’existe pas",
                    "en": "Does not exist",
                    "es": "No existe",
                    "pt": "Não existe",
                    "ar": "غير موجود"
                  }
                },
                {
                  "code": 98,
                  "label": {
                    "fr": "Ne sait pas",
                    "en": "Do not know",
                    "es": "No sabe",
                    "pt": "Não sabe",
                    "ar": "لا أعرف"
                  }
                },
                {
                  "code": 99,
                  "label": {
                    "fr": "Non applicable",
                    "en": "Not applicable",
                    "es": "No aplicable",
                    "pt": "Não aplicável",
                    "ar": "لا ينطبق"
                  }
                }
              ],
              "source": "Rapport méthodologique du 15 novembre 2026, pp. 19–22, QI09",
              "gate": {
                "field": "QI01_Participation",
                "op": "in",
                "values": [
                  1
                ]
              }
            },
            {
              "id": "QI09_D4",
              "label": {
                "fr": "Répertoire de chiffres avec sources et informations d’interprétation",
                "en": "Repository of figures with sources and interpretation information",
                "es": "Repositorio de cifras con fuentes e información de interpretación",
                "pt": "Repositório de números com fontes e informações de interpretação",
                "ar": "دليل أرقام مع مصادر ومعلومات تفسيرية"
              },
              "type": "single",
              "options": [
                {
                  "code": 1,
                  "label": {
                    "fr": "Existe et a été utilisé pour ces appuis pendant la période",
                    "en": "Exists and was used for this support during the period",
                    "es": "Existe y se utilizó para estos apoyos durante el período",
                    "pt": "Existe e foi utilizado para estes apoios durante o período",
                    "ar": "موجود واستخدم لهذا الدعم خلال الفترة"
                  }
                },
                {
                  "code": 2,
                  "label": {
                    "fr": "Existe mais n’a pas été utilisé pendant la période",
                    "en": "Exists but was not used during the period",
                    "es": "Existe pero no se utilizó durante el período",
                    "pt": "Existe mas não foi utilizado durante o período",
                    "ar": "موجود لكنه لم يستخدم خلال الفترة"
                  }
                },
                {
                  "code": 0,
                  "label": {
                    "fr": "N’existe pas",
                    "en": "Does not exist",
                    "es": "No existe",
                    "pt": "Não existe",
                    "ar": "غير موجود"
                  }
                },
                {
                  "code": 98,
                  "label": {
                    "fr": "Ne sait pas",
                    "en": "Do not know",
                    "es": "No sabe",
                    "pt": "Não sabe",
                    "ar": "لا أعرف"
                  }
                },
                {
                  "code": 99,
                  "label": {
                    "fr": "Non applicable",
                    "en": "Not applicable",
                    "es": "No aplicable",
                    "pt": "Não aplicável",
                    "ar": "لا ينطبق"
                  }
                }
              ],
              "source": "Rapport méthodologique du 15 novembre 2026, pp. 19–22, QI09",
              "gate": {
                "field": "QI01_Participation",
                "op": "in",
                "values": [
                  1
                ]
              }
            },
            {
              "id": "QI09_D5",
              "label": {
                "fr": "Trace du contrôle et de la validation statistique",
                "en": "Record of statistical checking and validation",
                "es": "Constancia del control y de la validación estadística",
                "pt": "Registo do controlo e da validação estatística",
                "ar": "إثبات المراجعة والتحقق الإحصائي"
              },
              "type": "single",
              "options": [
                {
                  "code": 1,
                  "label": {
                    "fr": "Existe et a été utilisé pour ces appuis pendant la période",
                    "en": "Exists and was used for this support during the period",
                    "es": "Existe y se utilizó para estos apoyos durante el período",
                    "pt": "Existe e foi utilizado para estes apoios durante o período",
                    "ar": "موجود واستخدم لهذا الدعم خلال الفترة"
                  }
                },
                {
                  "code": 2,
                  "label": {
                    "fr": "Existe mais n’a pas été utilisé pendant la période",
                    "en": "Exists but was not used during the period",
                    "es": "Existe pero no se utilizó durante el período",
                    "pt": "Existe mas não foi utilizado durante o período",
                    "ar": "موجود لكنه لم يستخدم خلال الفترة"
                  }
                },
                {
                  "code": 0,
                  "label": {
                    "fr": "N’existe pas",
                    "en": "Does not exist",
                    "es": "No existe",
                    "pt": "Não existe",
                    "ar": "غير موجود"
                  }
                },
                {
                  "code": 98,
                  "label": {
                    "fr": "Ne sait pas",
                    "en": "Do not know",
                    "es": "No sabe",
                    "pt": "Não sabe",
                    "ar": "لا أعرف"
                  }
                },
                {
                  "code": 99,
                  "label": {
                    "fr": "Non applicable",
                    "en": "Not applicable",
                    "es": "No aplicable",
                    "pt": "Não aplicável",
                    "ar": "لا ينطبق"
                  }
                }
              ],
              "source": "Rapport méthodologique du 15 novembre 2026, pp. 19–22, QI09",
              "gate": {
                "field": "QI01_Participation",
                "op": "in",
                "values": [
                  1
                ]
              }
            },
            {
              "id": "QI09_D6",
              "label": {
                "fr": "Conservation des versions et des corrections",
                "en": "Retention of versions and corrections",
                "es": "Conservación de versiones y correcciones",
                "pt": "Conservação de versões e correções",
                "ar": "حفظ النسخ والتصحيحات"
              },
              "type": "single",
              "options": [
                {
                  "code": 1,
                  "label": {
                    "fr": "Existe et a été utilisé pour ces appuis pendant la période",
                    "en": "Exists and was used for this support during the period",
                    "es": "Existe y se utilizó para estos apoyos durante el período",
                    "pt": "Existe e foi utilizado para estes apoios durante o período",
                    "ar": "موجود واستخدم لهذا الدعم خلال الفترة"
                  }
                },
                {
                  "code": 2,
                  "label": {
                    "fr": "Existe mais n’a pas été utilisé pendant la période",
                    "en": "Exists but was not used during the period",
                    "es": "Existe pero no se utilizó durante el período",
                    "pt": "Existe mas não foi utilizado durante o período",
                    "ar": "موجود لكنه لم يستخدم خلال الفترة"
                  }
                },
                {
                  "code": 0,
                  "label": {
                    "fr": "N’existe pas",
                    "en": "Does not exist",
                    "es": "No existe",
                    "pt": "Não existe",
                    "ar": "غير موجود"
                  }
                },
                {
                  "code": 98,
                  "label": {
                    "fr": "Ne sait pas",
                    "en": "Do not know",
                    "es": "No sabe",
                    "pt": "Não sabe",
                    "ar": "لا أعرف"
                  }
                },
                {
                  "code": 99,
                  "label": {
                    "fr": "Non applicable",
                    "en": "Not applicable",
                    "es": "No aplicable",
                    "pt": "Não aplicável",
                    "ar": "لا ينطبق"
                  }
                }
              ],
              "source": "Rapport méthodologique du 15 novembre 2026, pp. 19–22, QI09",
              "gate": {
                "field": "QI01_Participation",
                "op": "in",
                "values": [
                  1
                ]
              }
            },
            {
              "id": "QI09_D7",
              "label": {
                "fr": "Examen des délais, difficultés et retours des utilisateurs",
                "en": "Review of deadlines, difficulties and user feedback",
                "es": "Examen de plazos, dificultades y comentarios de usuarios",
                "pt": "Análise dos prazos, dificuldades e comentários dos utilizadores",
                "ar": "مراجعة المهل والصعوبات وملاحظات المستخدمين"
              },
              "type": "single",
              "options": [
                {
                  "code": 1,
                  "label": {
                    "fr": "Existe et a été utilisé pour ces appuis pendant la période",
                    "en": "Exists and was used for this support during the period",
                    "es": "Existe y se utilizó para estos apoyos durante el período",
                    "pt": "Existe e foi utilizado para estes apoios durante o período",
                    "ar": "موجود واستخدم لهذا الدعم خلال الفترة"
                  }
                },
                {
                  "code": 2,
                  "label": {
                    "fr": "Existe mais n’a pas été utilisé pendant la période",
                    "en": "Exists but was not used during the period",
                    "es": "Existe pero no se utilizó durante el período",
                    "pt": "Existe mas não foi utilizado durante o período",
                    "ar": "موجود لكنه لم يستخدم خلال الفترة"
                  }
                },
                {
                  "code": 0,
                  "label": {
                    "fr": "N’existe pas",
                    "en": "Does not exist",
                    "es": "No existe",
                    "pt": "Não existe",
                    "ar": "غير موجود"
                  }
                },
                {
                  "code": 98,
                  "label": {
                    "fr": "Ne sait pas",
                    "en": "Do not know",
                    "es": "No sabe",
                    "pt": "Não sabe",
                    "ar": "لا أعرف"
                  }
                },
                {
                  "code": 99,
                  "label": {
                    "fr": "Non applicable",
                    "en": "Not applicable",
                    "es": "No aplicable",
                    "pt": "Não aplicável",
                    "ar": "لا ينطبق"
                  }
                }
              ],
              "source": "Rapport méthodologique du 15 novembre 2026, pp. 19–22, QI09",
              "gate": {
                "field": "QI01_Participation",
                "op": "in",
                "values": [
                  1
                ]
              }
            },
            {
              "id": "QI09_Nouveaute",
              "label": {
                "fr": "Pratique nouvelle après le 31 octobre, facultatif : dispositif, date de mise en place et une phrase",
                "en": "New practice after 31 October, optional: arrangement, implementation date and one sentence",
                "es": "Práctica nueva después del 31 de octubre, opcional : dispositivo, fecha de implantación y una frase",
                "pt": "Prática nova após 31 de outubro, facultativo : dispositivo, data de implementação e uma frase",
                "ar": "ممارسة جديدة بعد 31 أكتوبر، اختياري : الترتيب وتاريخ تطبيقه وجملة توضيحية"
              },
              "type": "textarea",
              "options": [],
              "max_chars": 2500,
              "source": "Rapport méthodologique du 15 novembre 2026, pp. 19–22, QI09",
              "gate": {
                "field": "QI01_Participation",
                "op": "in",
                "values": [
                  1
                ]
              }
            }
          ],
          "note": {
            "fr": "",
            "en": "",
            "es": "",
            "pt": "",
            "ar": ""
          },
          "gate": null,
          "theme": "Suivi et moyens",
          "source": "Rapport méthodologique du 15 novembre 2026, pp. 19–22, QI09"
        },
        {
          "id": "QI10",
          "title": {
            "fr": "Besoins prioritaires",
            "en": "Priority needs",
            "es": "Necesidades prioritarias",
            "pt": "Necessidades prioritárias",
            "ar": "الاحتياجات ذات الأولوية"
          },
          "fields": [
            {
              "id": "QI10_P1",
              "label": {
                "fr": "Priorité 1",
                "en": "Priority 1",
                "es": "Prioridad 1",
                "pt": "Prioridade 1",
                "ar": "الأولوية 1"
              },
              "type": "single",
              "options": [
                {
                  "code": 1,
                  "label": {
                    "fr": "Clarifier le point d’entrée et les responsabilités",
                    "en": "Clarify the contact point and responsibilities",
                    "es": "Aclarar el punto de contacto y las responsabilidades",
                    "pt": "Clarificar o ponto de contacto e as responsabilidades",
                    "ar": "توضيح نقطة الاتصال والمسؤوليات"
                  }
                },
                {
                  "code": 2,
                  "label": {
                    "fr": "Mieux préciser et prioriser les demandes urgentes",
                    "en": "Clarify and prioritise urgent requests",
                    "es": "Precisar y priorizar las solicitudes urgentes",
                    "pt": "Precisar e priorizar os pedidos urgentes",
                    "ar": "تحديد الطلبات العاجلة وترتيب أولوياتها"
                  }
                },
                {
                  "code": 3,
                  "label": {
                    "fr": "Disposer d’un répertoire de chiffres documentés",
                    "en": "Have a repository of documented figures",
                    "es": "Disponer de un repositorio de cifras documentadas",
                    "pt": "Dispor de um repositório de números documentados",
                    "ar": "توفير دليل أرقام موثقة"
                  }
                },
                {
                  "code": 4,
                  "label": {
                    "fr": "Simplifier le contrôle et la validation",
                    "en": "Simplify checking and validation",
                    "es": "Simplificar el control y la validación",
                    "pt": "Simplificar o controlo e a validação",
                    "ar": "تبسيط المراجعة والتحقق"
                  }
                },
                {
                  "code": 5,
                  "label": {
                    "fr": "Préparer des messages et exemples de passages de discours",
                    "en": "Prepare messages and examples of speech passages",
                    "es": "Preparar mensajes y ejemplos de pasajes de discursos",
                    "pt": "Preparar mensagens e exemplos de trechos de discursos",
                    "ar": "إعداد رسائل وأمثلة لمقاطع الخطابات"
                  }
                },
                {
                  "code": 6,
                  "label": {
                    "fr": "Organiser les corrections et versions",
                    "en": "Organise corrections and versions",
                    "es": "Organizar las correcciones y las versiones",
                    "pt": "Organizar as correções e as versões",
                    "ar": "تنظيم التصحيحات والنسخ"
                  }
                },
                {
                  "code": 7,
                  "label": {
                    "fr": "Former les agents",
                    "en": "Train staff",
                    "es": "Formar al personal",
                    "pt": "Formar os agentes",
                    "ar": "تدريب الموظفين"
                  }
                },
                {
                  "code": 8,
                  "label": {
                    "fr": "Suivre les délais et les retours d’utilisation",
                    "en": "Monitor deadlines and user feedback",
                    "es": "Seguir los plazos y los comentarios sobre el uso",
                    "pt": "Acompanhar os prazos e os comentários sobre a utilização",
                    "ar": "متابعة المهل والملاحظات حول الاستخدام"
                  }
                },
                {
                  "code": 97,
                  "label": {
                    "fr": "Autre",
                    "en": "Other",
                    "es": "Otra",
                    "pt": "Outra",
                    "ar": "أخرى"
                  }
                },
                {
                  "code": 0,
                  "label": {
                    "fr": "Aucun besoin exprimé",
                    "en": "No need expressed",
                    "es": "Ninguna necesidad expresada",
                    "pt": "Nenhuma necessidade expressa",
                    "ar": "لم يعبر عن أي احتياج"
                  }
                },
                {
                  "code": 98,
                  "label": {
                    "fr": "Ne sait pas",
                    "en": "Do not know",
                    "es": "No sabe",
                    "pt": "Não sabe",
                    "ar": "لا أعرف"
                  }
                }
              ],
              "source": "Rapport méthodologique du 15 novembre 2026, pp. 19–22, QI10",
              "gate": {
                "field": "QI01_Participation",
                "op": "in",
                "values": [
                  1
                ]
              }
            },
            {
              "id": "QI10_P2",
              "label": {
                "fr": "Priorité 2",
                "en": "Priority 2",
                "es": "Prioridad 2",
                "pt": "Prioridade 2",
                "ar": "الأولوية 2"
              },
              "type": "single",
              "options": [
                {
                  "code": 1,
                  "label": {
                    "fr": "Clarifier le point d’entrée et les responsabilités",
                    "en": "Clarify the contact point and responsibilities",
                    "es": "Aclarar el punto de contacto y las responsabilidades",
                    "pt": "Clarificar o ponto de contacto e as responsabilidades",
                    "ar": "توضيح نقطة الاتصال والمسؤوليات"
                  }
                },
                {
                  "code": 2,
                  "label": {
                    "fr": "Mieux préciser et prioriser les demandes urgentes",
                    "en": "Clarify and prioritise urgent requests",
                    "es": "Precisar y priorizar las solicitudes urgentes",
                    "pt": "Precisar e priorizar os pedidos urgentes",
                    "ar": "تحديد الطلبات العاجلة وترتيب أولوياتها"
                  }
                },
                {
                  "code": 3,
                  "label": {
                    "fr": "Disposer d’un répertoire de chiffres documentés",
                    "en": "Have a repository of documented figures",
                    "es": "Disponer de un repositorio de cifras documentadas",
                    "pt": "Dispor de um repositório de números documentados",
                    "ar": "توفير دليل أرقام موثقة"
                  }
                },
                {
                  "code": 4,
                  "label": {
                    "fr": "Simplifier le contrôle et la validation",
                    "en": "Simplify checking and validation",
                    "es": "Simplificar el control y la validación",
                    "pt": "Simplificar o controlo e a validação",
                    "ar": "تبسيط المراجعة والتحقق"
                  }
                },
                {
                  "code": 5,
                  "label": {
                    "fr": "Préparer des messages et exemples de passages de discours",
                    "en": "Prepare messages and examples of speech passages",
                    "es": "Preparar mensajes y ejemplos de pasajes de discursos",
                    "pt": "Preparar mensagens e exemplos de trechos de discursos",
                    "ar": "إعداد رسائل وأمثلة لمقاطع الخطابات"
                  }
                },
                {
                  "code": 6,
                  "label": {
                    "fr": "Organiser les corrections et versions",
                    "en": "Organise corrections and versions",
                    "es": "Organizar las correcciones y las versiones",
                    "pt": "Organizar as correções e as versões",
                    "ar": "تنظيم التصحيحات والنسخ"
                  }
                },
                {
                  "code": 7,
                  "label": {
                    "fr": "Former les agents",
                    "en": "Train staff",
                    "es": "Formar al personal",
                    "pt": "Formar os agentes",
                    "ar": "تدريب الموظفين"
                  }
                },
                {
                  "code": 8,
                  "label": {
                    "fr": "Suivre les délais et les retours d’utilisation",
                    "en": "Monitor deadlines and user feedback",
                    "es": "Seguir los plazos y los comentarios sobre el uso",
                    "pt": "Acompanhar os prazos e os comentários sobre a utilização",
                    "ar": "متابعة المهل والملاحظات حول الاستخدام"
                  }
                },
                {
                  "code": 97,
                  "label": {
                    "fr": "Autre",
                    "en": "Other",
                    "es": "Otra",
                    "pt": "Outra",
                    "ar": "أخرى"
                  }
                }
              ],
              "gate": {
                "all": [
                  {
                    "field": "QI01_Participation",
                    "op": "in",
                    "values": [
                      1
                    ]
                  },
                  {
                    "field": "QI10_P1",
                    "op": "in",
                    "values": [
                      1,
                      2,
                      3,
                      4,
                      5,
                      6,
                      7,
                      8,
                      97
                    ]
                  }
                ]
              },
              "source": "Rapport méthodologique du 15 novembre 2026, pp. 19–22, QI10"
            },
            {
              "id": "QI10_P3",
              "label": {
                "fr": "Priorité 3",
                "en": "Priority 3",
                "es": "Prioridad 3",
                "pt": "Prioridade 3",
                "ar": "الأولوية 3"
              },
              "type": "single",
              "options": [
                {
                  "code": 1,
                  "label": {
                    "fr": "Clarifier le point d’entrée et les responsabilités",
                    "en": "Clarify the contact point and responsibilities",
                    "es": "Aclarar el punto de contacto y las responsabilidades",
                    "pt": "Clarificar o ponto de contacto e as responsabilidades",
                    "ar": "توضيح نقطة الاتصال والمسؤوليات"
                  }
                },
                {
                  "code": 2,
                  "label": {
                    "fr": "Mieux préciser et prioriser les demandes urgentes",
                    "en": "Clarify and prioritise urgent requests",
                    "es": "Precisar y priorizar las solicitudes urgentes",
                    "pt": "Precisar e priorizar os pedidos urgentes",
                    "ar": "تحديد الطلبات العاجلة وترتيب أولوياتها"
                  }
                },
                {
                  "code": 3,
                  "label": {
                    "fr": "Disposer d’un répertoire de chiffres documentés",
                    "en": "Have a repository of documented figures",
                    "es": "Disponer de un repositorio de cifras documentadas",
                    "pt": "Dispor de um repositório de números documentados",
                    "ar": "توفير دليل أرقام موثقة"
                  }
                },
                {
                  "code": 4,
                  "label": {
                    "fr": "Simplifier le contrôle et la validation",
                    "en": "Simplify checking and validation",
                    "es": "Simplificar el control y la validación",
                    "pt": "Simplificar o controlo e a validação",
                    "ar": "تبسيط المراجعة والتحقق"
                  }
                },
                {
                  "code": 5,
                  "label": {
                    "fr": "Préparer des messages et exemples de passages de discours",
                    "en": "Prepare messages and examples of speech passages",
                    "es": "Preparar mensajes y ejemplos de pasajes de discursos",
                    "pt": "Preparar mensagens e exemplos de trechos de discursos",
                    "ar": "إعداد رسائل وأمثلة لمقاطع الخطابات"
                  }
                },
                {
                  "code": 6,
                  "label": {
                    "fr": "Organiser les corrections et versions",
                    "en": "Organise corrections and versions",
                    "es": "Organizar las correcciones y las versiones",
                    "pt": "Organizar as correções e as versões",
                    "ar": "تنظيم التصحيحات والنسخ"
                  }
                },
                {
                  "code": 7,
                  "label": {
                    "fr": "Former les agents",
                    "en": "Train staff",
                    "es": "Formar al personal",
                    "pt": "Formar os agentes",
                    "ar": "تدريب الموظفين"
                  }
                },
                {
                  "code": 8,
                  "label": {
                    "fr": "Suivre les délais et les retours d’utilisation",
                    "en": "Monitor deadlines and user feedback",
                    "es": "Seguir los plazos y los comentarios sobre el uso",
                    "pt": "Acompanhar os prazos e os comentários sobre a utilização",
                    "ar": "متابعة المهل والملاحظات حول الاستخدام"
                  }
                },
                {
                  "code": 97,
                  "label": {
                    "fr": "Autre",
                    "en": "Other",
                    "es": "Otra",
                    "pt": "Outra",
                    "ar": "أخرى"
                  }
                }
              ],
              "gate": {
                "all": [
                  {
                    "field": "QI01_Participation",
                    "op": "in",
                    "values": [
                      1
                    ]
                  },
                  {
                    "field": "QI10_P1",
                    "op": "in",
                    "values": [
                      1,
                      2,
                      3,
                      4,
                      5,
                      6,
                      7,
                      8,
                      97
                    ]
                  }
                ]
              },
              "source": "Rapport méthodologique du 15 novembre 2026, pp. 19–22, QI10"
            },
            {
              "id": "QI10_Autre",
              "label": {
                "fr": "Autre priorité : précisez",
                "en": "Other priority: please specify",
                "es": "Otra prioridad : especifique",
                "pt": "Outra prioridade : especifique",
                "ar": "أولوية أخرى : يرجى التحديد"
              },
              "type": "text",
              "options": [],
              "max_chars": 500,
              "gate": {
                "all": [
                  {
                    "field": "QI01_Participation",
                    "op": "in",
                    "values": [
                      1
                    ]
                  },
                  {
                    "any": [
                      {
                        "field": "QI10_P1",
                        "op": "in",
                        "values": [
                          97
                        ]
                      },
                      {
                        "field": "QI10_P2",
                        "op": "in",
                        "values": [
                          97
                        ]
                      },
                      {
                        "field": "QI10_P3",
                        "op": "in",
                        "values": [
                          97
                        ]
                      }
                    ]
                  }
                ]
              },
              "source": "Rapport méthodologique du 15 novembre 2026, pp. 19–22, QI10"
            },
            {
              "id": "QI10_Difficulte",
              "label": {
                "fr": "Pour la priorité 1, quelle difficulté réelle cela permettrait-il de résoudre ? Au plus 40 mots ; sinon, écrire « besoin anticipé ».",
                "en": "For priority 1, what actual difficulty would this resolve? Up to 40 words; otherwise write ‘anticipated need’.",
                "es": "Para la prioridad 1, ¿qué dificultad real se resolvería? Máximo 40 palabras; en su defecto, escriba « necesidad prevista ».",
                "pt": "Para a prioridade 1, que dificuldade real seria resolvida? Máximo de 40 palavras; caso contrário, escreva « necessidade prevista ».",
                "ar": "بالنسبة للأولوية 1، ما الصعوبة الفعلية التي ستحل؟ في حدود 40 كلمة، أو اكتب « احتياج متوقع »."
              },
              "type": "textarea",
              "options": [],
              "max_chars": 2500,
              "max_words": 40,
              "source": "Rapport méthodologique du 15 novembre 2026, pp. 19–22, QI10",
              "gate": {
                "field": "QI01_Participation",
                "op": "in",
                "values": [
                  1
                ]
              }
            },
            {
              "id": "QI10_Action",
              "label": {
                "fr": "Première action possible avec les moyens existants",
                "en": "First action possible with existing resources",
                "es": "Primera acción posible con los recursos existentes",
                "pt": "Primeira ação possível com os recursos existentes",
                "ar": "أول إجراء ممكن بالموارد المتاحة"
              },
              "type": "text",
              "options": [],
              "max_chars": 500,
              "source": "Rapport méthodologique du 15 novembre 2026, pp. 19–22, QI10",
              "gate": {
                "field": "QI01_Participation",
                "op": "in",
                "values": [
                  1
                ]
              }
            },
            {
              "id": "QI10_Responsable",
              "label": {
                "fr": "Fonction pouvant la conduire",
                "en": "Function that could lead the action",
                "es": "Función que podría dirigir la acción",
                "pt": "Função que poderia conduzir a ação",
                "ar": "الوظيفة التي يمكن أن تقود الإجراء"
              },
              "type": "text",
              "options": [],
              "max_chars": 500,
              "source": "Rapport méthodologique du 15 novembre 2026, pp. 19–22, QI10",
              "gate": {
                "field": "QI01_Participation",
                "op": "in",
                "values": [
                  1
                ]
              }
            },
            {
              "id": "QI10_Moyen",
              "label": {
                "fr": "Moyen déjà disponible",
                "en": "Resource already available",
                "es": "Recurso ya disponible",
                "pt": "Recurso já disponível",
                "ar": "المورد المتاح بالفعل"
              },
              "type": "text",
              "options": [],
              "max_chars": 500,
              "source": "Rapport méthodologique du 15 novembre 2026, pp. 19–22, QI10",
              "gate": {
                "field": "QI01_Participation",
                "op": "in",
                "values": [
                  1
                ]
              }
            },
            {
              "id": "QI10_Obstacle",
              "label": {
                "fr": "Principal obstacle",
                "en": "Main obstacle",
                "es": "Principal obstáculo",
                "pt": "Principal obstáculo",
                "ar": "العائق الرئيسي"
              },
              "type": "text",
              "options": [],
              "max_chars": 500,
              "source": "Rapport méthodologique du 15 novembre 2026, pp. 19–22, QI10",
              "gate": {
                "field": "QI01_Participation",
                "op": "in",
                "values": [
                  1
                ]
              }
            }
          ],
          "note": {
            "fr": "Classer au plus trois codes différents. Une ou deux priorités suffisent. 0 et 98 sont exclusifs des autres priorités. Indiquer « à déterminer » si nécessaire pour l’action, le responsable, les moyens ou l’obstacle.",
            "en": "Rank up to three different codes. One or two priorities are sufficient. Codes 0 and 98 exclude other priorities. Write ‘to be determined’ where necessary for the action, lead function, resources or obstacle.",
            "es": "Clasifique hasta tres códigos distintos. Una o dos prioridades bastan. Los códigos 0 y 98 excluyen las demás prioridades. Escriba « por determinar » cuando sea necesario para la acción, función responsable, recursos u obstáculo.",
            "pt": "Ordene até três códigos distintos. Uma ou duas prioridades bastam. Os códigos 0 e 98 excluem as restantes prioridades. Escreva « a determinar » quando necessário para a ação, função responsável, recursos ou obstáculo.",
            "ar": "رتب ثلاثة رموز مختلفة كحد أقصى. تكفي أولوية أو أولويتان. يستبعد الرمزان 0 و98 بقية الأولويات. اكتب « يحدد لاحقا » عند الحاجة للإجراء أو الوظيفة المسؤولة أو الموارد أو العائق."
          },
          "gate": null,
          "theme": "Suivi et moyens",
          "source": "Rapport méthodologique du 15 novembre 2026, pp. 19–22, QI10"
        },
        {
          "id": "QI11",
          "title": {
            "fr": "Un exemple ou document partageable",
            "en": "One shareable example or document",
            "es": "Un ejemplo o documento que pueda compartirse",
            "pt": "Um exemplo ou documento que possa ser partilhado",
            "ar": "مثال أو وثيقة قابلة للمشاركة"
          },
          "fields": [
            {
              "id": "QI11_Type",
              "label": {
                "fr": "Exemple proposé, facultatif",
                "en": "Example provided, optional",
                "es": "Ejemplo facilitado, opcional",
                "pt": "Exemplo fornecido, facultativo",
                "ar": "المثال المقدم، اختياري"
              },
              "type": "single",
              "options": [
                {
                  "code": 1,
                  "label": {
                    "fr": "Lien public",
                    "en": "Public link",
                    "es": "Enlace público",
                    "pt": "Ligação pública",
                    "ar": "رابط متاح للعموم"
                  }
                },
                {
                  "code": 2,
                  "label": {
                    "fr": "Extrait non confidentiel dont le partage est autorisé",
                    "en": "Non-confidential extract authorised for sharing",
                    "es": "Extracto no confidencial cuya difusión está autorizada",
                    "pt": "Excerto não confidencial cuja partilha está autorizada",
                    "ar": "مقتطف غير سري مصرح بمشاركته"
                  }
                },
                {
                  "code": 0,
                  "label": {
                    "fr": "Aucun exemple partagé",
                    "en": "No example shared",
                    "es": "No se comparte ningún ejemplo",
                    "pt": "Nenhum exemplo partilhado",
                    "ar": "لم يشارك أي مثال"
                  }
                }
              ],
              "source": "Rapport méthodologique du 15 novembre 2026, pp. 19–22, QI11",
              "gate": {
                "field": "QI01_Participation",
                "op": "in",
                "values": [
                  1
                ]
              }
            },
            {
              "id": "QI11_Reference",
              "label": {
                "fr": "Titre ou description, date et lien public si disponible ; extrait autorisé le cas échéant",
                "en": "Title or description, date and public link if available; authorised extract where relevant",
                "es": "Título o descripción, fecha y enlace público si existe; extracto autorizado, si procede",
                "pt": "Título ou descrição, data e ligação pública se disponível; excerto autorizado, se aplicável",
                "ar": "العنوان أو الوصف والتاريخ والرابط العام إن وجد، ومقتطف مصرح به عند الاقتضاء"
              },
              "type": "textarea",
              "options": [],
              "max_chars": 2500,
              "gate": {
                "all": [
                  {
                    "field": "QI01_Participation",
                    "op": "in",
                    "values": [
                      1
                    ]
                  },
                  {
                    "field": "QI11_Type",
                    "op": "in",
                    "values": [
                      1,
                      2
                    ]
                  }
                ]
              },
              "source": "Rapport méthodologique du 15 novembre 2026, pp. 19–22, QI11"
            },
            {
              "id": "QI11_Apport",
              "label": {
                "fr": "Ce que cet exemple montre et ce qui pourrait être adapté ailleurs, au plus 60 mots",
                "en": "What this example demonstrates and what could be adapted elsewhere, up to 60 words",
                "es": "Qué demuestra el ejemplo y qué podría adaptarse en otro contexto, hasta 60 palabras",
                "pt": "O que o exemplo demonstra e o que poderia ser adaptado noutro contexto, até 60 palavras",
                "ar": "ما الذي يوضحه المثال وما الذي يمكن تكييفه في سياق آخر، حتى 60 كلمة"
              },
              "type": "textarea",
              "options": [],
              "max_chars": 2500,
              "max_words": 60,
              "gate": {
                "all": [
                  {
                    "field": "QI01_Participation",
                    "op": "in",
                    "values": [
                      1
                    ]
                  },
                  {
                    "field": "QI11_Type",
                    "op": "in",
                    "values": [
                      1,
                      2
                    ]
                  }
                ]
              },
              "source": "Rapport méthodologique du 15 novembre 2026, pp. 19–22, QI11"
            }
          ],
          "note": {
            "fr": "Proposer un seul exemple : procédure, fiche de chiffres, modèle de demande, contrôle, correction ou discours public. Ne pas joindre de données individuelles, brouillon confidentiel, coordonnées personnelles ou contenu non autorisé. Un exemple antérieur peut illustrer une pratique, mais sa date doit rester visible.",
            "en": "Provide only one example: procedure, fact sheet, request template, check, correction or public speech. Do not provide individual-level data, confidential drafts, personal contact details or unauthorised content. An older example can illustrate a practice, but its date must remain visible.",
            "es": "Proporcione un solo ejemplo : procedimiento, ficha, modelo de solicitud, control, corrección o discurso público. No incluya datos individuales, borradores confidenciales, contactos personales ni contenido no autorizado. Un ejemplo antiguo puede ilustrar una práctica, pero su fecha debe quedar visible.",
            "pt": "Forneça apenas um exemplo : procedimento, ficha, modelo de pedido, controlo, correção ou discurso público. Não inclua dados individuais, rascunhos confidenciais, contactos pessoais ou conteúdo não autorizado. Um exemplo antigo pode ilustrar uma prática, mas a data deve ficar visível.",
            "ar": "قدم مثالا واحدا فقط : إجراء أو بطاقة أو نموذج طلب أو مراجعة أو تصحيح أو خطاب عام. لا تقدم بيانات فردية أو مسودات سرية أو بيانات اتصال شخصية أو محتوى غير مصرح به. يمكن لمثال قديم توضيح ممارسة، لكن يجب إبقاء تاريخه ظاهرا."
          },
          "gate": null,
          "theme": "Suivi et moyens",
          "source": "Rapport méthodologique du 15 novembre 2026, pp. 19–22, QI11"
        },
        {
          "id": "QI12",
          "title": {
            "fr": "Attribution et contact facultatif",
            "en": "Attribution and optional contact",
            "es": "Atribución y contacto opcional",
            "pt": "Atribuição e contacto facultativo",
            "ar": "الإسناد وبيانات الاتصال الاختيارية"
          },
          "fields": [
            {
              "id": "QI12_Attribution",
              "label": {
                "fr": "Modalité autorisée pour la contribution et l’exemple partagé",
                "en": "Permitted use of the contribution and shared example",
                "es": "Uso autorizado de la contribución y del ejemplo compartido",
                "pt": "Utilização autorizada da contribuição e do exemplo partilhado",
                "ar": "الاستخدام المصرح به للمساهمة والمثال المشترك"
              },
              "type": "single",
              "options": [
                {
                  "code": 0,
                  "label": {
                    "fr": "Synthèse anonymisée seulement, sans attribution identifiable",
                    "en": "Anonymised summary only, with no identifiable attribution",
                    "es": "Solo síntesis anonimizada, sin atribución identificable",
                    "pt": "Apenas síntese anonimizada, sem atribuição identificável",
                    "ar": "ملخص دون تحديد الهوية فقط، دون إسناد كاشف للهوية"
                  }
                },
                {
                  "code": 1,
                  "label": {
                    "fr": "Attribution à l’institution, sans citation textuelle identifiable",
                    "en": "Attribution to the institution, without an identifiable quotation",
                    "es": "Atribución a la institución, sin cita textual identificable",
                    "pt": "Atribuição à instituição, sem citação textual identificável",
                    "ar": "الإسناد إلى المؤسسة دون اقتباس نصي كاشف للهوية"
                  }
                },
                {
                  "code": 2,
                  "label": {
                    "fr": "Attribution à l’institution et citation textuelle",
                    "en": "Attribution to the institution and direct quotation",
                    "es": "Atribución a la institución y cita textual",
                    "pt": "Atribuição à instituição e citação textual",
                    "ar": "الإسناد إلى المؤسسة والاقتباس النصي"
                  }
                },
                {
                  "code": 3,
                  "label": {
                    "fr": "Me soumettre le passage avant toute attribution ou citation identifiable",
                    "en": "Submit the passage to me before any identifiable attribution or quotation",
                    "es": "Enviarme el pasaje antes de cualquier atribución o cita identificable",
                    "pt": "Enviar-me o trecho antes de qualquer atribuição ou citação identificável",
                    "ar": "اعرض علي المقطع قبل أي إسناد أو اقتباس كاشف للهوية"
                  }
                }
              ],
              "attribution": true,
              "source": "Rapport méthodologique du 15 novembre 2026, pp. 19–22, QI12",
              "gate": {
                "field": "QI01_Participation",
                "op": "in",
                "values": [
                  1
                ]
              }
            },
            {
              "id": "QI12_Suite",
              "label": {
                "fr": "Accepteriez-vous un bref complément ou un test d’outil ?",
                "en": "Would you agree to a brief follow-up or a tool test?",
                "es": "¿Aceptaría una breve aclaración o una prueba de herramienta?",
                "pt": "Aceitaria um breve esclarecimento ou um teste de ferramenta?",
                "ar": "هل تقبل استكمالا موجزا أو اختبار أداة؟"
              },
              "type": "single",
              "options": [
                {
                  "code": 1,
                  "label": {
                    "fr": "Complément",
                    "en": "Follow-up clarification",
                    "es": "Complemento",
                    "pt": "Complemento",
                    "ar": "استكمال"
                  }
                },
                {
                  "code": 2,
                  "label": {
                    "fr": "Test",
                    "en": "Tool test",
                    "es": "Prueba de herramienta",
                    "pt": "Teste de ferramenta",
                    "ar": "اختبار أداة"
                  }
                },
                {
                  "code": 3,
                  "label": {
                    "fr": "Les deux",
                    "en": "Both",
                    "es": "Ambos",
                    "pt": "Ambos",
                    "ar": "كلاهما"
                  }
                },
                {
                  "code": 0,
                  "label": {
                    "fr": "Non",
                    "en": "No",
                    "es": "No",
                    "pt": "Não",
                    "ar": "لا"
                  }
                },
                {
                  "code": 98,
                  "label": {
                    "fr": "À déterminer",
                    "en": "To be determined",
                    "es": "Por determinar",
                    "pt": "A determinar",
                    "ar": "يحدد لاحقا"
                  }
                }
              ],
              "source": "Rapport méthodologique du 15 novembre 2026, pp. 19–22, QI12",
              "gate": {
                "field": "QI01_Participation",
                "op": "in",
                "values": [
                  1
                ]
              }
            },
            {
              "id": "QI12_Contact_fonction",
              "label": {
                "fr": "Contact facultatif : fonction ou service à recontacter",
                "en": "Optional contact: function or service to contact",
                "es": "Contacto opcional : función o servicio al que contactar",
                "pt": "Contacto facultativo : função ou serviço a contactar",
                "ar": "جهة اتصال اختيارية : الوظيفة أو الإدارة التي يمكن التواصل معها"
              },
              "type": "text",
              "options": [],
              "max_chars": 500,
              "private": true,
              "source": "Rapport méthodologique du 15 novembre 2026, pp. 19–22, QI12",
              "gate": {
                "field": "QI01_Participation",
                "op": "in",
                "values": [
                  1
                ]
              }
            },
            {
              "id": "QI12_Contact_courriel",
              "label": {
                "fr": "Courriel institutionnel ou générique, facultatif",
                "en": "Institutional or generic email address, optional",
                "es": "Correo institucional o genérico, opcional",
                "pt": "Correio institucional ou genérico, facultativo",
                "ar": "بريد مؤسسي أو عام، اختياري"
              },
              "type": "email",
              "options": [],
              "max_chars": 254,
              "private": true,
              "source": "Rapport méthodologique du 15 novembre 2026, pp. 19–22, QI12",
              "gate": {
                "field": "QI01_Participation",
                "op": "in",
                "values": [
                  1
                ]
              }
            },
            {
              "id": "QI12_Contact_relais",
              "label": {
                "fr": "Préférer un relais par STATAFRIC, facultatif",
                "en": "Prefer contact through STATAFRIC, optional",
                "es": "Preferir contacto a través de STATAFRIC, opcional",
                "pt": "Preferir contacto através do STATAFRIC, facultativo",
                "ar": "تفضيل التواصل عبر STATAFRIC، اختياري"
              },
              "type": "single",
              "options": [
                {
                  "code": 1,
                  "label": {
                    "fr": "Passer par STATAFRIC",
                    "en": "Contact through STATAFRIC",
                    "es": "Contacto a través de STATAFRIC",
                    "pt": "Contacto através do STATAFRIC",
                    "ar": "التواصل عبر STATAFRIC"
                  }
                }
              ],
              "private": true,
              "source": "Rapport méthodologique du 15 novembre 2026, pp. 19–22, QI12",
              "gate": {
                "field": "QI01_Participation",
                "op": "in",
                "values": [
                  1
                ]
              }
            }
          ],
          "note": {
            "fr": "Les choix 1 et 2 supposent l’autorisation correspondante. Une contribution individuelle sans cette autorisation reste anonymisée. Sans choix explicite, la synthèse anonymisée (code 0) s’applique. L’absence de retour ne vaut jamais accord de citation.",
            "en": "Options 1 and 2 require the corresponding authorisation. An individual contribution without that authorisation remains anonymised. Without an explicit choice, an anonymised summary (code 0) applies. Silence never constitutes permission to quote.",
            "es": "Las opciones 1 y 2 requieren la autorización correspondiente. Una contribución individual sin esa autorización permanece anonimizada. Sin selección explícita, se aplica la síntesis anonimizada (código 0). El silencio nunca autoriza una cita.",
            "pt": "As opções 1 e 2 exigem a autorização correspondente. Uma contribuição individual sem essa autorização mantém-se anonimizada. Sem escolha explícita, aplica-se a síntese anonimizada (código 0). O silêncio nunca autoriza uma citação.",
            "ar": "يتطلب الخياران 1 و2 التصريح المناسب. تبقى المساهمة الفردية دون هذا التصريح مجهولة الهوية. عند غياب اختيار صريح، يطبق الملخص دون تحديد الهوية (الرمز 0). الصمت لا يعني أبدا الإذن بالاقتباس."
          },
          "gate": null,
          "theme": "Suivi et moyens",
          "source": "Rapport méthodologique du 15 novembre 2026, pp. 19–22, QI12"
        }
      ],
      "consent": "QI01_Participation",
      "attribution": "QI12_Attribution",
      "institution": "QI01_INS",
      "country": "QI01_Pays",
      "status": "QI01_Statut",
      "previous": "QI01_Reference_precedente",
      "version": "1.2 — 2026-11-15",
      "unit": "QI01_Unite",
      "sector": "QI01_Domaine",
      "response_language": "QI01_Langue_texte",
      "origin": "QI01_Origine_invitation",
      "relay": "QI01_INS_relais",
      "profile": "INS",
      "priority_prefix": "QI10",
      "relay_note": {
        "fr": "INS : transmettre le lien producteurs à 3–8 organismes et le lien utilisateurs à 4–6 équipes. Ne pas collecter ni approuver leur réponse. Les autres producteurs répondent QP ; les équipes utilisatrices répondent QU.",
        "en": "NSOs: forward the producer link to 3–8 organisations and the user link to 4–6 teams. Do not collect or approve their responses. Other producers complete QP; user teams complete QU.",
        "es": "ONE : transmita el enlace de productores a 3–8 organismos y el de usuarios a 4–6 equipos. No recoja ni apruebe sus respuestas. Los otros productores completan QP; los equipos usuarios completan QU.",
        "pt": "INE : encaminhe a ligação dos produtores a 3–8 organismos e a dos utilizadores a 4–6 equipas. Não recolha nem aprove as respostas. Os outros produtores preenchem QP; as equipas utilizadoras preenchem QU.",
        "ar": "للأجهزة الوطنية للإحصاء : أحيلوا رابط المنتجين إلى 3–8 جهات ورابط المستخدمين إلى 4–6 فرق. لا تجمعوا إجاباتهم ولا تعتمدوا محتواها. يجيب المنتجون الآخرون عن QP وفرق المستخدمين عن QU."
      }
    },
    "producteurs": {
      "title": {
        "fr": "Autres producteurs de statistiques",
        "en": "Other producers of statistics",
        "es": "Otros productores de estadísticas",
        "pt": "Outros produtores de estatísticas",
        "ar": "الجهات الأخرى المنتجة للإحصاءات"
      },
      "duration": {
        "fr": "15 à 20 minutes",
        "en": "15 to 20 minutes",
        "es": "De 15 a 20 minutos",
        "pt": "15 a 20 minutos",
        "ar": "من 15 إلى 20 دقيقة"
      },
      "sections": [
        {
          "id": "QP01",
          "title": {
            "fr": "Accord, profil et périmètre de la contribution",
            "en": "Consent, profile and scope of the contribution",
            "es": "Consentimiento, perfil y ámbito de la contribución",
            "pt": "Consentimento, perfil e âmbito da contribuição",
            "ar": "الموافقة والصفة ونطاق المساهمة"
          },
          "fields": [
            {
              "id": "QP01_Participation",
              "label": {
                "fr": "Acceptez-vous l’utilisation de votre contribution selon la notice ?",
                "en": "Do you agree to the use of your contribution as described in the notice?",
                "es": "¿Autoriza el uso de su contribución según el aviso de participación?",
                "pt": "Autoriza a utilização da sua contribuição conforme o aviso de participação?",
                "ar": "هل توافق على استخدام مساهمتك وفق إشعار المشاركة؟"
              },
              "type": "single",
              "options": [
                {
                  "code": 1,
                  "label": {
                    "fr": "J’accepte",
                    "en": "I agree",
                    "es": "Acepto",
                    "pt": "Aceito",
                    "ar": "أوافق"
                  }
                },
                {
                  "code": 0,
                  "label": {
                    "fr": "Je ne participe pas — fin du questionnaire",
                    "en": "I do not wish to participate — end of questionnaire",
                    "es": "No deseo participar — fin del cuestionario",
                    "pt": "Não desejo participar — fim do questionário",
                    "ar": "لا أرغب في المشاركة — نهاية الاستبيان"
                  }
                }
              ],
              "required": true,
              "source": "Rapport méthodologique, annexe 1bis, p. 23"
            },
            {
              "id": "QP01_Produit",
              "label": {
                "fr": "Votre organisme produit-il des statistiques ou des indicateurs utilisables pour l’action publique ?",
                "en": "Does your organisation produce statistics or indicators usable for public action?",
                "es": "¿Produce su organismo estadísticas o indicadores utilizables para la acción pública?",
                "pt": "O seu organismo produz estatísticas ou indicadores utilizáveis na ação pública?",
                "ar": "هل تنتج مؤسستكم إحصاءات أو مؤشرات قابلة للاستخدام في العمل العام؟"
              },
              "type": "single",
              "options": [
                {
                  "code": 1,
                  "label": {
                    "fr": "Oui",
                    "en": "Yes",
                    "es": "Sí",
                    "pt": "Sim",
                    "ar": "نعم"
                  }
                },
                {
                  "code": 0,
                  "label": {
                    "fr": "Non",
                    "en": "No",
                    "es": "No",
                    "pt": "Não",
                    "ar": "لا"
                  }
                },
                {
                  "code": 98,
                  "label": {
                    "fr": "Ne sait pas",
                    "en": "Do not know",
                    "es": "No sabe",
                    "pt": "Não sabe",
                    "ar": "لا أعرف"
                  }
                }
              ],
              "source": "Rapport méthodologique, annexe 1bis, p. 23",
              "gate": {
                "field": "QP01_Participation",
                "op": "in",
                "values": [
                  1
                ]
              }
            },
            {
              "id": "QP01_Pays",
              "label": {
                "fr": "Pays",
                "en": "Country",
                "es": "País",
                "pt": "País",
                "ar": "البلد"
              },
              "type": "text",
              "options": [],
              "max_chars": 500,
              "gate": {
                "all": [
                  {
                    "field": "QP01_Participation",
                    "op": "in",
                    "values": [
                      1
                    ]
                  },
                  {
                    "field": "QP01_Produit",
                    "op": "not_in",
                    "values": [
                      0
                    ]
                  }
                ]
              },
              "source": "Rapport méthodologique, annexe 1bis, p. 23"
            },
            {
              "id": "QP01_Institution",
              "label": {
                "fr": "Institution de rattachement",
                "en": "Parent institution",
                "es": "Institución de adscripción",
                "pt": "Instituição de tutela",
                "ar": "المؤسسة التي تتبعها الجهة"
              },
              "type": "text",
              "options": [],
              "max_chars": 500,
              "gate": {
                "all": [
                  {
                    "field": "QP01_Participation",
                    "op": "in",
                    "values": [
                      1
                    ]
                  },
                  {
                    "field": "QP01_Produit",
                    "op": "not_in",
                    "values": [
                      0
                    ]
                  }
                ]
              },
              "source": "Rapport méthodologique, annexe 1bis, p. 23"
            },
            {
              "id": "QP01_Unite",
              "label": {
                "fr": "Service ou unité de production",
                "en": "Production service or unit",
                "es": "Servicio o unidad de producción",
                "pt": "Serviço ou unidade de produção",
                "ar": "إدارة أو وحدة الإنتاج"
              },
              "type": "text",
              "options": [],
              "max_chars": 500,
              "gate": {
                "all": [
                  {
                    "field": "QP01_Participation",
                    "op": "in",
                    "values": [
                      1
                    ]
                  },
                  {
                    "field": "QP01_Produit",
                    "op": "not_in",
                    "values": [
                      0
                    ]
                  }
                ]
              },
              "source": "Rapport méthodologique, annexe 1bis, p. 23"
            },
            {
              "id": "QP01_Type",
              "label": {
                "fr": "Type principal d’organisme",
                "en": "Main type of organisation",
                "es": "Tipo principal de organismo",
                "pt": "Tipo principal de organismo",
                "ar": "النوع الرئيسي للجهة"
              },
              "type": "single",
              "options": [
                {
                  "code": 1,
                  "label": {
                    "fr": "Ministère ou service statistique sectoriel",
                    "en": "Ministry or sectoral statistical unit",
                    "es": "Ministerio o servicio estadístico sectorial",
                    "pt": "Ministério ou serviço estatístico setorial",
                    "ar": "وزارة أو إدارة إحصائية قطاعية"
                  }
                },
                {
                  "code": 2,
                  "label": {
                    "fr": "Banque centrale ou institution monétaire",
                    "en": "Central bank or monetary institution",
                    "es": "Banco central o institución monetaria",
                    "pt": "Banco central ou instituição monetária",
                    "ar": "بنك مركزي أو مؤسسة نقدية"
                  }
                },
                {
                  "code": 3,
                  "label": {
                    "fr": "Autre administration ou organisme de données administratives",
                    "en": "Other administration or administrative-data body",
                    "es": "Otra administración u organismo de datos administrativos",
                    "pt": "Outra administração ou organismo de dados administrativos",
                    "ar": "إدارة أخرى أو جهة بيانات إدارية"
                  }
                },
                {
                  "code": 4,
                  "label": {
                    "fr": "Agence ou observatoire public",
                    "en": "Public agency or observatory",
                    "es": "Agencia u observatorio público",
                    "pt": "Agência ou observatório público",
                    "ar": "وكالة أو مرصد عام"
                  }
                },
                {
                  "code": 5,
                  "label": {
                    "fr": "Collectivité ou service territorial",
                    "en": "Local authority or territorial service",
                    "es": "Entidad local o servicio territorial",
                    "pt": "Autarquia ou serviço territorial",
                    "ar": "جماعة محلية أو إدارة إقليمية"
                  }
                },
                {
                  "code": 6,
                  "label": {
                    "fr": "Organisme de recherche",
                    "en": "Research organisation",
                    "es": "Organismo de investigación",
                    "pt": "Organismo de investigação",
                    "ar": "جهة بحثية"
                  }
                },
                {
                  "code": 7,
                  "label": {
                    "fr": "Organisme privé ou associatif producteur",
                    "en": "Private or non-profit producer",
                    "es": "Productor privado o asociativo",
                    "pt": "Produtor privado ou associativo",
                    "ar": "جهة منتجة خاصة أو أهلية"
                  }
                },
                {
                  "code": 97,
                  "label": {
                    "fr": "Autre",
                    "en": "Other",
                    "es": "Otra",
                    "pt": "Outra",
                    "ar": "أخرى"
                  }
                }
              ],
              "gate": {
                "all": [
                  {
                    "field": "QP01_Participation",
                    "op": "in",
                    "values": [
                      1
                    ]
                  },
                  {
                    "field": "QP01_Produit",
                    "op": "not_in",
                    "values": [
                      0
                    ]
                  }
                ]
              },
              "source": "Rapport méthodologique, annexe 1bis, p. 23"
            },
            {
              "id": "QP01_Type_Autre",
              "label": {
                "fr": "Autre : précisez",
                "en": "Other: please specify",
                "es": "Otra : especifique",
                "pt": "Outra : especifique",
                "ar": "أخرى : يرجى التحديد"
              },
              "type": "text",
              "options": [],
              "max_chars": 500,
              "gate": {
                "all": [
                  {
                    "field": "QP01_Participation",
                    "op": "in",
                    "values": [
                      1
                    ]
                  },
                  {
                    "all": [
                      {
                        "field": "QP01_Produit",
                        "op": "not_in",
                        "values": [
                          0
                        ]
                      },
                      {
                        "field": "QP01_Type",
                        "op": "in",
                        "values": [
                          97
                        ]
                      }
                    ]
                  }
                ]
              },
              "source": "Rapport méthodologique, annexe 1bis, p. 23"
            },
            {
              "id": "QP01_Domaine",
              "label": {
                "fr": "Domaine principal",
                "en": "Main domain",
                "es": "Ámbito principal",
                "pt": "Domínio principal",
                "ar": "المجال الرئيسي"
              },
              "type": "single",
              "options": [
                {
                  "code": 1,
                  "label": {
                    "fr": "Économie, finances ou monnaie",
                    "en": "Economy, finance or monetary affairs",
                    "es": "Economía, finanzas o moneda",
                    "pt": "Economia, finanças ou moeda",
                    "ar": "الاقتصاد أو المالية أو النقد"
                  }
                },
                {
                  "code": 2,
                  "label": {
                    "fr": "Santé ou protection sociale",
                    "en": "Health or social protection",
                    "es": "Salud o protección social",
                    "pt": "Saúde ou proteção social",
                    "ar": "الصحة أو الحماية الاجتماعية"
                  }
                },
                {
                  "code": 3,
                  "label": {
                    "fr": "Éducation ou formation",
                    "en": "Education or training",
                    "es": "Educación o formación",
                    "pt": "Educação ou formação",
                    "ar": "التعليم أو التدريب"
                  }
                },
                {
                  "code": 4,
                  "label": {
                    "fr": "Agriculture ou alimentation",
                    "en": "Agriculture or food",
                    "es": "Agricultura o alimentación",
                    "pt": "Agricultura ou alimentação",
                    "ar": "الزراعة أو الغذاء"
                  }
                },
                {
                  "code": 5,
                  "label": {
                    "fr": "Emploi ou conditions de vie",
                    "en": "Employment or living conditions",
                    "es": "Empleo o condiciones de vida",
                    "pt": "Emprego ou condições de vida",
                    "ar": "التشغيل أو ظروف المعيشة"
                  }
                },
                {
                  "code": 6,
                  "label": {
                    "fr": "Environnement, énergie ou infrastructures",
                    "en": "Environment, energy or infrastructure",
                    "es": "Medio ambiente, energía o infraestructuras",
                    "pt": "Ambiente, energia ou infraestruturas",
                    "ar": "البيئة أو الطاقة أو البنية التحتية"
                  }
                },
                {
                  "code": 7,
                  "label": {
                    "fr": "Population, état civil ou territoire",
                    "en": "Population, civil registration or territory",
                    "es": "Población, registro civil o territorio",
                    "pt": "População, registo civil ou território",
                    "ar": "السكان أو الحالة المدنية أو المجال الجغرافي"
                  }
                },
                {
                  "code": 8,
                  "label": {
                    "fr": "Plusieurs domaines sans domaine principal",
                    "en": "Multiple domains with no main domain",
                    "es": "Varios ámbitos sin uno principal",
                    "pt": "Vários domínios sem um principal",
                    "ar": "مجالات متعددة دون مجال رئيسي"
                  }
                },
                {
                  "code": 97,
                  "label": {
                    "fr": "Autre",
                    "en": "Other",
                    "es": "Otra",
                    "pt": "Outra",
                    "ar": "أخرى"
                  }
                }
              ],
              "gate": {
                "all": [
                  {
                    "field": "QP01_Participation",
                    "op": "in",
                    "values": [
                      1
                    ]
                  },
                  {
                    "field": "QP01_Produit",
                    "op": "not_in",
                    "values": [
                      0
                    ]
                  }
                ]
              },
              "source": "Rapport méthodologique, annexe 1bis, p. 23"
            },
            {
              "id": "QP01_Domaine_Autre",
              "label": {
                "fr": "Autre : précisez",
                "en": "Other: please specify",
                "es": "Otra : especifique",
                "pt": "Outra : especifique",
                "ar": "أخرى : يرجى التحديد"
              },
              "type": "text",
              "options": [],
              "max_chars": 500,
              "gate": {
                "all": [
                  {
                    "field": "QP01_Participation",
                    "op": "in",
                    "values": [
                      1
                    ]
                  },
                  {
                    "all": [
                      {
                        "field": "QP01_Produit",
                        "op": "not_in",
                        "values": [
                          0
                        ]
                      },
                      {
                        "field": "QP01_Domaine",
                        "op": "in",
                        "values": [
                          97
                        ]
                      }
                    ]
                  }
                ]
              },
              "source": "Rapport méthodologique, annexe 1bis, p. 23"
            },
            {
              "id": "QP01_Domaines_autres",
              "label": {
                "fr": "Autres domaines, facultatif",
                "en": "Other domains, optional",
                "es": "Otros ámbitos, opcional",
                "pt": "Outros domínios, facultativo",
                "ar": "مجالات أخرى، اختياري"
              },
              "type": "text",
              "options": [],
              "max_chars": 500,
              "gate": {
                "all": [
                  {
                    "field": "QP01_Participation",
                    "op": "in",
                    "values": [
                      1
                    ]
                  },
                  {
                    "field": "QP01_Produit",
                    "op": "not_in",
                    "values": [
                      0
                    ]
                  }
                ]
              },
              "source": "Rapport méthodologique, annexe 1bis, p. 23"
            },
            {
              "id": "QP01_Statut",
              "label": {
                "fr": "Statut de la réponse",
                "en": "Response status",
                "es": "Carácter de la respuesta",
                "pt": "Estatuto da resposta",
                "ar": "صفة الإجابة"
              },
              "type": "single",
              "options": [
                {
                  "code": 1,
                  "label": {
                    "fr": "Réponse institutionnelle consolidée",
                    "en": "Consolidated institutional response",
                    "es": "Respuesta institucional consolidada",
                    "pt": "Resposta institucional consolidada",
                    "ar": "إجابة مؤسسية موحدة"
                  }
                },
                {
                  "code": 2,
                  "label": {
                    "fr": "Contribution d’un service, non consolidée au niveau de l’institution",
                    "en": "Service-level contribution, not consolidated at institution level",
                    "es": "Contribución de un servicio, no consolidada a nivel institucional",
                    "pt": "Contribuição de um serviço, não consolidada ao nível da instituição",
                    "ar": "مساهمة من إدارة، غير موحدة على مستوى المؤسسة"
                  }
                },
                {
                  "code": 3,
                  "label": {
                    "fr": "Contribution individuelle fondée sur ma pratique professionnelle",
                    "en": "Individual contribution based on my professional practice",
                    "es": "Contribución individual basada en mi práctica profesional",
                    "pt": "Contribuição individual baseada na minha prática profissional",
                    "ar": "مساهمة فردية تستند إلى ممارستي المهنية"
                  }
                }
              ],
              "gate": {
                "all": [
                  {
                    "field": "QP01_Participation",
                    "op": "in",
                    "values": [
                      1
                    ]
                  },
                  {
                    "field": "QP01_Produit",
                    "op": "not_in",
                    "values": [
                      0
                    ]
                  }
                ]
              },
              "source": "Rapport méthodologique, annexe 1bis, p. 23"
            },
            {
              "id": "QP01_Fonction",
              "label": {
                "fr": "Fonction, sans nom personnel",
                "en": "Function, without a personal name",
                "es": "Función, sin nombre personal",
                "pt": "Função, sem nome pessoal",
                "ar": "الوظيفة، دون اسم شخصي"
              },
              "type": "single",
              "options": [
                {
                  "code": 1,
                  "label": {
                    "fr": "Production",
                    "en": "Production",
                    "es": "Producción",
                    "pt": "Produção",
                    "ar": "الإنتاج"
                  }
                },
                {
                  "code": 2,
                  "label": {
                    "fr": "Méthodes ou qualité",
                    "en": "Methods or quality",
                    "es": "Métodos o calidad",
                    "pt": "Métodos ou qualidade",
                    "ar": "المنهجيات أو الجودة"
                  }
                },
                {
                  "code": 3,
                  "label": {
                    "fr": "Communication ou diffusion",
                    "en": "Communication or dissemination",
                    "es": "Comunicación o difusión",
                    "pt": "Comunicação ou divulgação",
                    "ar": "الاتصال أو النشر"
                  }
                },
                {
                  "code": 4,
                  "label": {
                    "fr": "Coordination ou direction",
                    "en": "Coordination or management",
                    "es": "Coordinación o dirección",
                    "pt": "Coordenação ou direção",
                    "ar": "التنسيق أو الإدارة العليا"
                  }
                },
                {
                  "code": 97,
                  "label": {
                    "fr": "Autre",
                    "en": "Other",
                    "es": "Otra",
                    "pt": "Outra",
                    "ar": "أخرى"
                  }
                }
              ],
              "gate": {
                "all": [
                  {
                    "field": "QP01_Participation",
                    "op": "in",
                    "values": [
                      1
                    ]
                  },
                  {
                    "field": "QP01_Produit",
                    "op": "not_in",
                    "values": [
                      0
                    ]
                  }
                ]
              },
              "source": "Rapport méthodologique, annexe 1bis, p. 23"
            },
            {
              "id": "QP01_Fonction_Autre",
              "label": {
                "fr": "Autre : précisez",
                "en": "Other: please specify",
                "es": "Otra : especifique",
                "pt": "Outra : especifique",
                "ar": "أخرى : يرجى التحديد"
              },
              "type": "text",
              "options": [],
              "max_chars": 500,
              "gate": {
                "all": [
                  {
                    "field": "QP01_Participation",
                    "op": "in",
                    "values": [
                      1
                    ]
                  },
                  {
                    "all": [
                      {
                        "field": "QP01_Produit",
                        "op": "not_in",
                        "values": [
                          0
                        ]
                      },
                      {
                        "field": "QP01_Fonction",
                        "op": "in",
                        "values": [
                          97
                        ]
                      }
                    ]
                  }
                ]
              },
              "source": "Rapport méthodologique, annexe 1bis, p. 23"
            },
            {
              "id": "QP01_Envoi",
              "label": {
                "fr": "Nature de l’envoi",
                "en": "Type of submission",
                "es": "Tipo de envío",
                "pt": "Tipo de envio",
                "ar": "نوع الإرسال"
              },
              "type": "single",
              "options": [
                {
                  "code": 1,
                  "label": {
                    "fr": "Première réponse",
                    "en": "Initial response",
                    "es": "Primera respuesta",
                    "pt": "Primeira resposta",
                    "ar": "الإجابة الأولى"
                  }
                },
                {
                  "code": 2,
                  "label": {
                    "fr": "Complément",
                    "en": "Supplement",
                    "es": "Complemento",
                    "pt": "Complemento",
                    "ar": "استكمال"
                  }
                }
              ],
              "gate": {
                "all": [
                  {
                    "field": "QP01_Participation",
                    "op": "in",
                    "values": [
                      1
                    ]
                  },
                  {
                    "field": "QP01_Produit",
                    "op": "not_in",
                    "values": [
                      0
                    ]
                  }
                ]
              },
              "source": "Rapport méthodologique, annexe 1bis, p. 23"
            },
            {
              "id": "QP01_Reference_precedente",
              "label": {
                "fr": "Référence ou date de l’envoi précédent, si connue",
                "en": "Reference or date of the previous submission, if known",
                "es": "Referencia o fecha del envío anterior, si se conoce",
                "pt": "Referência ou data do envio anterior, se conhecida",
                "ar": "مرجع الإرسال السابق أو تاريخه، إن كان معروفا"
              },
              "type": "text",
              "options": [],
              "max_chars": 500,
              "gate": {
                "all": [
                  {
                    "field": "QP01_Participation",
                    "op": "in",
                    "values": [
                      1
                    ]
                  },
                  {
                    "all": [
                      {
                        "field": "QP01_Produit",
                        "op": "not_in",
                        "values": [
                          0
                        ]
                      },
                      {
                        "field": "QP01_Envoi",
                        "op": "in",
                        "values": [
                          2
                        ]
                      }
                    ]
                  }
                ]
              },
              "source": "Rapport méthodologique, annexe 1bis, p. 23"
            },
            {
              "id": "QP01_Origine_invitation",
              "label": {
                "fr": "Origine du lien",
                "en": "Origin of the link",
                "es": "Origen del enlace",
                "pt": "Origem da ligação",
                "ar": "مصدر الرابط"
              },
              "type": "single",
              "options": [
                {
                  "code": 1,
                  "label": {
                    "fr": "INS",
                    "en": "NSO",
                    "es": "ONE",
                    "pt": "INE",
                    "ar": "الجهاز الوطني للإحصاء"
                  }
                },
                {
                  "code": 2,
                  "label": {
                    "fr": "STATAFRIC ou consultant",
                    "en": "STATAFRIC or consultant",
                    "es": "STATAFRIC o consultor",
                    "pt": "STATAFRIC ou consultor",
                    "ar": "STATAFRIC أو الاستشاري"
                  }
                },
                {
                  "code": 3,
                  "label": {
                    "fr": "Autre relais",
                    "en": "Other forwarding organisation",
                    "es": "Otra entidad que transmitió la invitación",
                    "pt": "Outra entidade que encaminhou o convite",
                    "ar": "جهة أخرى أحالت الدعوة"
                  }
                },
                {
                  "code": 4,
                  "label": {
                    "fr": "Accès spontané",
                    "en": "Unsolicited access",
                    "es": "Acceso espontáneo",
                    "pt": "Acesso espontâneo",
                    "ar": "وصول تلقائي دون دعوة مباشرة"
                  }
                },
                {
                  "code": 98,
                  "label": {
                    "fr": "Ne sait pas",
                    "en": "Do not know",
                    "es": "No sabe",
                    "pt": "Não sabe",
                    "ar": "لا أعرف"
                  }
                }
              ],
              "gate": {
                "all": [
                  {
                    "field": "QP01_Participation",
                    "op": "in",
                    "values": [
                      1
                    ]
                  },
                  {
                    "field": "QP01_Produit",
                    "op": "not_in",
                    "values": [
                      0
                    ]
                  }
                ]
              },
              "source": "Rapport méthodologique, annexe 1bis, p. 23"
            },
            {
              "id": "QP01_INS_relais",
              "label": {
                "fr": "INS ou organisme relais, facultatif",
                "en": "Forwarding NSO or organisation, optional",
                "es": "ONE o entidad que transmitió la invitación, opcional",
                "pt": "INE ou entidade que encaminhou o convite, facultativo",
                "ar": "الجهاز الوطني للإحصاء أو الجهة المحيلة، اختياري"
              },
              "type": "text",
              "options": [],
              "max_chars": 500,
              "gate": {
                "all": [
                  {
                    "field": "QP01_Participation",
                    "op": "in",
                    "values": [
                      1
                    ]
                  },
                  {
                    "field": "QP01_Produit",
                    "op": "not_in",
                    "values": [
                      0
                    ]
                  }
                ]
              },
              "source": "Rapport méthodologique, annexe 1bis, p. 23"
            },
            {
              "id": "QP01_Langue_texte",
              "label": {
                "fr": "Langue principale de vos réponses libres",
                "en": "Main language of your free-text responses",
                "es": "Idioma principal de sus respuestas abiertas",
                "pt": "Língua principal das suas respostas abertas",
                "ar": "اللغة الأساسية لإجاباتك الحرة"
              },
              "type": "single",
              "options": [
                {
                  "code": "fr",
                  "label": {
                    "fr": "Français",
                    "en": "French",
                    "es": "Francés",
                    "pt": "Francês",
                    "ar": "الفرنسية"
                  }
                },
                {
                  "code": "en",
                  "label": {
                    "fr": "Anglais",
                    "en": "English",
                    "es": "Inglés",
                    "pt": "Inglês",
                    "ar": "الإنجليزية"
                  }
                },
                {
                  "code": "ar",
                  "label": {
                    "fr": "Arabe",
                    "en": "Arabic",
                    "es": "Árabe",
                    "pt": "Árabe",
                    "ar": "العربية"
                  }
                },
                {
                  "code": "pt",
                  "label": {
                    "fr": "Portugais",
                    "en": "Portuguese",
                    "es": "Portugués",
                    "pt": "Português",
                    "ar": "البرتغالية"
                  }
                },
                {
                  "code": "es",
                  "label": {
                    "fr": "Espagnol",
                    "en": "Spanish",
                    "es": "Español",
                    "pt": "Espanhol",
                    "ar": "الإسبانية"
                  }
                }
              ],
              "gate": {
                "all": [
                  {
                    "field": "QP01_Participation",
                    "op": "in",
                    "values": [
                      1
                    ]
                  },
                  {
                    "field": "QP01_Produit",
                    "op": "not_in",
                    "values": [
                      0
                    ]
                  }
                ]
              },
              "source": "Rapport méthodologique, annexe 1bis, p. 23"
            }
          ],
          "note": {
            "fr": "Producteurs hors INS uniquement. Les services internes de l’INS utilisent QI. Un simple transfert de fichiers administratifs sans traitement statistique ne suffit pas. Si votre organisme ne produit pas de statistiques, utilisez QU pour un rôle d’utilisateur ou demandez conseil au consultant. Une participation ne confère pas un statut officiel aux statistiques de l’organisme.",
            "en": "For producers outside the NSO only. Internal NSO units use QI. Simply forwarding administrative files without statistical processing is not sufficient. If your organisation does not produce statistics, use QU for a user role or seek advice from the consultant. Participation does not confer official status on the organisation’s statistics.",
            "es": "Solo para productores externos a la ONE. Las unidades internas de la ONE usan QI. La simple transmisión de archivos administrativos sin tratamiento estadístico no basta. Si su organismo no produce estadísticas, use QU para su función de usuario o consulte al consultor. La participación no confiere carácter oficial a sus estadísticas.",
            "pt": "Apenas para produtores externos ao INE. As unidades internas do INE usam QI. O simples envio de ficheiros administrativos sem tratamento estatístico não basta. Se o organismo não produz estatísticas, use QU para a função de utilizador ou consulte o consultor. A participação não confere caráter oficial às estatísticas do organismo.",
            "ar": "للمنتجين خارج الجهاز الوطني للإحصاء فقط. تستخدم إدارات الجهاز الداخلية QI. لا يكفي مجرد إحالة ملفات إدارية دون معالجة إحصائية. إذا لم تنتج مؤسستكم إحصاءات، استخدموا QU لدور المستخدم أو استشيروا الاستشاري. المشاركة لا تمنح إحصاءات المؤسسة صفة رسمية."
          },
          "gate": null,
          "theme": "Suivi et moyens",
          "source": "Rapport méthodologique, annexe 1bis, p. 23"
        },
        {
          "id": "QP02",
          "title": {
            "fr": "Appuis reçus pendant la période de référence",
            "en": "Support requests received during the reference period",
            "es": "Solicitudes de apoyo recibidas durante el período de referencia",
            "pt": "Pedidos de apoio recebidos no período de referência",
            "ar": "طلبات الدعم المستلمة خلال الفترة المرجعية"
          },
          "fields": [
            {
              "id": "QP02a",
              "label": {
                "fr": "Combien de demandes distinctes avez-vous recensées dans le périmètre de votre réponse ?",
                "en": "How many distinct requests did you identify within the scope of your response?",
                "es": "¿Cuántas solicitudes distintas ha identificado en el ámbito de su respuesta?",
                "pt": "Quantos pedidos distintos identificou no âmbito da sua resposta?",
                "ar": "كم طلبا منفصلا أحصيتم ضمن نطاق إجابتكم؟"
              },
              "type": "single",
              "options": [
                {
                  "code": 0,
                  "label": {
                    "fr": "Aucune / aucun",
                    "en": "None",
                    "es": "Ninguna",
                    "pt": "Nenhum",
                    "ar": "لا يوجد"
                  }
                },
                {
                  "code": 1,
                  "label": {
                    "fr": "De 1 à 5",
                    "en": "1 to 5",
                    "es": "De 1 a 5",
                    "pt": "De 1 a 5",
                    "ar": "من 1 إلى 5"
                  }
                },
                {
                  "code": 2,
                  "label": {
                    "fr": "De 6 à 20",
                    "en": "6 to 20",
                    "es": "De 6 a 20",
                    "pt": "De 6 a 20",
                    "ar": "من 6 إلى 20"
                  }
                },
                {
                  "code": 3,
                  "label": {
                    "fr": "De 21 à 50",
                    "en": "21 to 50",
                    "es": "De 21 a 50",
                    "pt": "De 21 a 50",
                    "ar": "من 21 إلى 50"
                  }
                },
                {
                  "code": 4,
                  "label": {
                    "fr": "Plus de 50",
                    "en": "More than 50",
                    "es": "Más de 50",
                    "pt": "Mais de 50",
                    "ar": "أكثر من 50"
                  }
                },
                {
                  "code": 98,
                  "label": {
                    "fr": "Ne sait pas",
                    "en": "Do not know",
                    "es": "No sabe",
                    "pt": "Não sabe",
                    "ar": "لا أعرف"
                  }
                }
              ],
              "gate": {
                "all": [
                  {
                    "field": "QP01_Participation",
                    "op": "in",
                    "values": [
                      1
                    ]
                  },
                  {
                    "field": "QP01_Produit",
                    "op": "not_in",
                    "values": [
                      0
                    ]
                  }
                ]
              },
              "source": "Rapport méthodologique, annexe 1bis, pp. 23–27, QP02"
            },
            {
              "id": "QP02b",
              "label": {
                "fr": "Certaines de ces demandes exigeaient-elles une réponse dans les 24 heures ou moins suivant leur réception ?",
                "en": "Did any of these requests require a response within 24 hours of receipt?",
                "es": "¿Alguna de estas solicitudes exigía respuesta en las 24 horas siguientes a su recepción?",
                "pt": "Algum destes pedidos exigia resposta nas 24 horas seguintes à receção?",
                "ar": "هل تطلب أي من هذه الطلبات إجابة خلال 24 ساعة أو أقل من استلامه؟"
              },
              "type": "single",
              "options": [
                {
                  "code": 1,
                  "label": {
                    "fr": "Oui",
                    "en": "Yes",
                    "es": "Sí",
                    "pt": "Sim",
                    "ar": "نعم"
                  }
                },
                {
                  "code": 0,
                  "label": {
                    "fr": "Non",
                    "en": "No",
                    "es": "No",
                    "pt": "Não",
                    "ar": "لا"
                  }
                },
                {
                  "code": 98,
                  "label": {
                    "fr": "Ne sait pas",
                    "en": "Do not know",
                    "es": "No sabe",
                    "pt": "Não sabe",
                    "ar": "لا أعرف"
                  }
                },
                {
                  "code": 99,
                  "label": {
                    "fr": "Non applicable, aucune demande",
                    "en": "Not applicable, no requests",
                    "es": "No aplicable, ninguna solicitud",
                    "pt": "Não aplicável, nenhum pedido",
                    "ar": "لا ينطبق، لا توجد طلبات"
                  }
                }
              ],
              "gate": {
                "all": [
                  {
                    "field": "QP01_Participation",
                    "op": "in",
                    "values": [
                      1
                    ]
                  },
                  {
                    "field": "QP01_Produit",
                    "op": "not_in",
                    "values": [
                      0
                    ]
                  }
                ]
              },
              "source": "Rapport méthodologique, annexe 1bis, pp. 23–27, QP02"
            },
            {
              "id": "QP02c",
              "label": {
                "fr": "Sur quoi repose cette réponse ?",
                "en": "What is this response based on?",
                "es": "¿En qué se basa esta respuesta?",
                "pt": "Em que se baseia esta resposta?",
                "ar": "على ماذا تستند هذه الإجابة؟"
              },
              "type": "single",
              "options": [
                {
                  "code": 1,
                  "label": {
                    "fr": "Registre ou inventaire consulté",
                    "en": "Register or inventory consulted",
                    "es": "Registro o inventario consultado",
                    "pt": "Registo ou inventário consultado",
                    "ar": "سجل أو حصر تمت مراجعته"
                  }
                },
                {
                  "code": 2,
                  "label": {
                    "fr": "Estimation fondée sur la pratique connue",
                    "en": "Estimate based on known practice",
                    "es": "Estimación basada en la práctica conocida",
                    "pt": "Estimativa baseada na prática conhecida",
                    "ar": "تقدير يستند إلى الممارسة المعروفة"
                  }
                },
                {
                  "code": 98,
                  "label": {
                    "fr": "Ne sait pas",
                    "en": "Do not know",
                    "es": "No sabe",
                    "pt": "Não sabe",
                    "ar": "لا أعرف"
                  }
                }
              ],
              "gate": {
                "all": [
                  {
                    "field": "QP01_Participation",
                    "op": "in",
                    "values": [
                      1
                    ]
                  },
                  {
                    "field": "QP01_Produit",
                    "op": "not_in",
                    "values": [
                      0
                    ]
                  }
                ]
              },
              "source": "Rapport méthodologique, annexe 1bis, pp. 23–27, QP02"
            },
            {
              "id": "QP02d",
              "label": {
                "fr": "Séparément, votre organisme a-t-il fourni des statistiques pour une note ou un arbitrage de décision sans lien identifié avec un discours ?",
                "en": "Separately, did your organisation provide statistics for a decision note or decision-making process with no identified link to a speech?",
                "es": "Por separado, ¿proporcionó su organismo estadísticas para una nota o decisión sin vínculo identificado con un discurso?",
                "pt": "Separadamente, o seu organismo forneceu estatísticas para uma nota ou decisão sem ligação identificada a um discurso?",
                "ar": "بشكل منفصل، هل قدمت مؤسستكم إحصاءات لمذكرة أو مفاضلة لاتخاذ قرار دون ارتباط محدد بخطاب؟"
              },
              "type": "single",
              "options": [
                {
                  "code": 1,
                  "label": {
                    "fr": "Oui",
                    "en": "Yes",
                    "es": "Sí",
                    "pt": "Sim",
                    "ar": "نعم"
                  }
                },
                {
                  "code": 0,
                  "label": {
                    "fr": "Non",
                    "en": "No",
                    "es": "No",
                    "pt": "Não",
                    "ar": "لا"
                  }
                },
                {
                  "code": 98,
                  "label": {
                    "fr": "Ne sait pas",
                    "en": "Do not know",
                    "es": "No sabe",
                    "pt": "Não sabe",
                    "ar": "لا أعرف"
                  }
                }
              ],
              "gate": {
                "all": [
                  {
                    "field": "QP01_Participation",
                    "op": "in",
                    "values": [
                      1
                    ]
                  },
                  {
                    "field": "QP01_Produit",
                    "op": "not_in",
                    "values": [
                      0
                    ]
                  }
                ]
              },
              "source": "Rapport méthodologique, annexe 1bis, pp. 23–27, QP02"
            },
            {
              "id": "QP02d_Theme",
              "label": {
                "fr": "Si oui, thème ou type de décision, sans détail confidentiel, une phrase. Ne pas ajouter ces appuis au nombre de demandes de discours.",
                "en": "If yes, give the topic or type of decision, without confidential details, in one sentence. Do not add this support to the number of speech requests.",
                "es": "Si respondió sí, indique tema o tipo de decisión en una frase, sin detalles confidenciales. No añada estos apoyos al número de solicitudes de discursos.",
                "pt": "Se sim, indique tema ou tipo de decisão numa frase, sem pormenores confidenciais. Não acrescente estes apoios ao número de pedidos de discursos.",
                "ar": "إذا كانت الإجابة نعم، اذكر الموضوع أو نوع القرار في جملة دون تفاصيل سرية. لا تضف هذا الدعم إلى عدد طلبات الخطابات."
              },
              "type": "textarea",
              "options": [],
              "max_chars": 2500,
              "gate": {
                "all": [
                  {
                    "field": "QP01_Participation",
                    "op": "in",
                    "values": [
                      1
                    ]
                  },
                  {
                    "all": [
                      {
                        "field": "QP01_Produit",
                        "op": "not_in",
                        "values": [
                          0
                        ]
                      },
                      {
                        "field": "QP02d",
                        "op": "in",
                        "values": [
                          1
                        ]
                      }
                    ]
                  }
                ]
              },
              "source": "Rapport méthodologique, annexe 1bis, pp. 23–27, QP02"
            }
          ],
          "note": {
            "fr": "Une demande concerne un discours ou une intervention orale, reçue directement ou par l’INS. Compter une seule fois les échanges d’un même appui. Exclure les demandes générales sans lien connu avec un discours. Si QP02a = 0, passer à QP08 après QP02d. Si le volume est inconnu, un cas réel peut être décrit. Plusieurs producteurs peuvent participer au même discours : ne pas additionner leurs demandes en un total national.",
            "en": "A request concerns a speech or oral intervention, received directly or through the NSO. Count exchanges for the same support only once. Exclude general requests with no known link to a speech. If QP02a = 0, go to QP08 after QP02d. A real case may be described when volume is unknown. Multiple producers may support the same speech: do not add their requests into a national total.",
            "es": "La solicitud se refiere a un discurso o intervención oral, recibida directamente o por la ONE. Cuente una sola vez los intercambios del mismo apoyo. Excluya solicitudes generales sin vínculo conocido con un discurso. Si QP02a = 0, pase a QP08 después de QP02d. Puede describirse un caso real aunque el volumen sea desconocido. Varios productores pueden intervenir en el mismo discurso : no sume sus solicitudes para obtener un total nacional.",
            "pt": "O pedido refere-se a um discurso ou intervenção oral, recebido diretamente ou pelo INE. Conte uma só vez as trocas do mesmo apoio. Exclua pedidos gerais sem ligação conhecida a um discurso. Se QP02a = 0, avance para QP08 após QP02d. Pode descrever-se um caso real mesmo com volume desconhecido. Vários produtores podem apoiar o mesmo discurso : não some os pedidos num total nacional.",
            "ar": "يتعلق الطلب بخطاب أو مداخلة شفهية، ويستلم مباشرة أو عبر الجهاز الوطني للإحصاء. احسب تبادلات الدعم الواحد مرة واحدة. استبعد الطلبات العامة غير المرتبطة بخطاب معروف. إذا كان QP02a = 0، انتقل إلى QP08 بعد QP02d. يمكن وصف حالة فعلية حتى إن جهل العدد. قد يدعم عدة منتجين الخطاب نفسه : لا تجمع طلباتهم لاستخراج مجموع وطني."
          },
          "gate": null,
          "theme": "Demande et urgence",
          "source": "Rapport méthodologique, annexe 1bis, pp. 23–27, QP02"
        },
        {
          "id": "QP03",
          "title": {
            "fr": "Dernière demande clôturée connue",
            "en": "Most recent known closed request",
            "es": "Última solicitud cerrada conocida",
            "pt": "Último pedido encerrado conhecido",
            "ar": "أحدث طلب مغلق معروف"
          },
          "fields": [
            {
              "id": "QP03_Situation",
              "label": {
                "fr": "Situation",
                "en": "Situation",
                "es": "Situación",
                "pt": "Situação",
                "ar": "الحالة"
              },
              "type": "single",
              "options": [
                {
                  "code": 1,
                  "label": {
                    "fr": "Des chiffres ont été transmis",
                    "en": "Figures were provided",
                    "es": "Se proporcionaron cifras",
                    "pt": "Foram fornecidos números",
                    "ar": "تم تقديم أرقام"
                  }
                },
                {
                  "code": 2,
                  "label": {
                    "fr": "La demande a été clôturée sans fourniture de chiffres",
                    "en": "The request was closed without providing figures",
                    "es": "La solicitud se cerró sin proporcionar cifras",
                    "pt": "O pedido foi encerrado sem fornecimento de números",
                    "ar": "أغلق الطلب دون تقديم أرقام"
                  }
                },
                {
                  "code": 3,
                  "label": {
                    "fr": "Aucune demande clôturée connue pendant la période",
                    "en": "No known request closed during the period",
                    "es": "Ninguna solicitud cerrada conocida durante el período",
                    "pt": "Nenhum pedido encerrado conhecido durante o período",
                    "ar": "لا أعرف طلبا أغلق خلال الفترة"
                  }
                },
                {
                  "code": 98,
                  "label": {
                    "fr": "Je ne dispose pas des informations nécessaires",
                    "en": "I do not have the necessary information",
                    "es": "No dispongo de la información necesaria",
                    "pt": "Não disponho das informações necessárias",
                    "ar": "لا تتوفر لدي المعلومات اللازمة"
                  }
                }
              ],
              "gate": {
                "all": [
                  {
                    "field": "QP01_Participation",
                    "op": "in",
                    "values": [
                      1
                    ]
                  },
                  {
                    "field": "QP01_Produit",
                    "op": "not_in",
                    "values": [
                      0
                    ]
                  }
                ]
              },
              "source": "Rapport méthodologique, annexe 1bis, pp. 23–27, QP03"
            },
            {
              "id": "QP03_Mois",
              "label": {
                "fr": "Mois et année de clôture, MM/AAAA",
                "en": "Month and year of closure, MM/YYYY",
                "es": "Mes y año de cierre, MM/AAAA",
                "pt": "Mês e ano do encerramento, MM/AAAA",
                "ar": "شهر وسنة الإغلاق، MM/YYYY"
              },
              "type": "month",
              "options": [],
              "max_chars": 7,
              "gate": {
                "all": [
                  {
                    "field": "QP01_Participation",
                    "op": "in",
                    "values": [
                      1
                    ]
                  },
                  {
                    "all": [
                      {
                        "field": "QP01_Produit",
                        "op": "not_in",
                        "values": [
                          0
                        ]
                      },
                      {
                        "field": "QP03_Situation",
                        "op": "in",
                        "values": [
                          1,
                          2
                        ]
                      }
                    ]
                  }
                ]
              },
              "source": "Rapport méthodologique, annexe 1bis, pp. 23–27, QP03"
            },
            {
              "id": "QP03_Theme",
              "label": {
                "fr": "Thème général, sans nom de personne ni titre confidentiel du discours",
                "en": "General topic, without a person’s name or a confidential speech title",
                "es": "Tema general, sin nombre de persona ni título confidencial del discurso",
                "pt": "Tema geral, sem nome de pessoa nem título confidencial do discurso",
                "ar": "الموضوع العام، دون اسم شخص أو عنوان سري للخطاب"
              },
              "type": "text",
              "options": [],
              "max_chars": 500,
              "gate": {
                "all": [
                  {
                    "field": "QP01_Participation",
                    "op": "in",
                    "values": [
                      1
                    ]
                  },
                  {
                    "all": [
                      {
                        "field": "QP01_Produit",
                        "op": "not_in",
                        "values": [
                          0
                        ]
                      },
                      {
                        "field": "QP03_Situation",
                        "op": "in",
                        "values": [
                          1,
                          2
                        ]
                      }
                    ]
                  }
                ]
              },
              "source": "Rapport méthodologique, annexe 1bis, pp. 23–27, QP03"
            },
            {
              "id": "QP03_Demandeur",
              "label": {
                "fr": "Demandeur final connu",
                "en": "Known final requester",
                "es": "Solicitante final conocido",
                "pt": "Requerente final conhecido",
                "ar": "الجهة الطالبة النهائية المعروفة"
              },
              "type": "single",
              "options": [
                {
                  "code": 1,
                  "label": {
                    "fr": "Cabinet ou équipe d’un décideur national",
                    "en": "Office or team of a national decision-maker",
                    "es": "Gabinete o equipo de un decisor nacional",
                    "pt": "Gabinete ou equipa de um decisor nacional",
                    "ar": "مكتب أو فريق صانع قرار وطني"
                  }
                },
                {
                  "code": 2,
                  "label": {
                    "fr": "Ministère ou administration sectorielle",
                    "en": "Ministry or sectoral administration",
                    "es": "Ministerio o administración sectorial",
                    "pt": "Ministério ou administração setorial",
                    "ar": "وزارة أو إدارة قطاعية"
                  }
                },
                {
                  "code": 3,
                  "label": {
                    "fr": "Autorité territoriale",
                    "en": "Territorial authority",
                    "es": "Autoridad territorial",
                    "pt": "Autoridade territorial",
                    "ar": "سلطة إقليمية"
                  }
                },
                {
                  "code": 4,
                  "label": {
                    "fr": "Direction de votre organisme",
                    "en": "Your organisation’s management",
                    "es": "Dirección de su organismo",
                    "pt": "Direção do seu organismo",
                    "ar": "إدارة مؤسستكم"
                  }
                },
                {
                  "code": 5,
                  "label": {
                    "fr": "INS",
                    "en": "NSO",
                    "es": "ONE",
                    "pt": "INE",
                    "ar": "الجهاز الوطني للإحصاء"
                  }
                },
                {
                  "code": 6,
                  "label": {
                    "fr": "Institution régionale ou internationale",
                    "en": "Regional or international institution",
                    "es": "Institución regional o internacional",
                    "pt": "Instituição regional ou internacional",
                    "ar": "مؤسسة إقليمية أو دولية"
                  }
                },
                {
                  "code": 97,
                  "label": {
                    "fr": "Autre",
                    "en": "Other",
                    "es": "Otra",
                    "pt": "Outra",
                    "ar": "أخرى"
                  }
                },
                {
                  "code": 98,
                  "label": {
                    "fr": "Ne sait pas",
                    "en": "Do not know",
                    "es": "No sabe",
                    "pt": "Não sabe",
                    "ar": "لا أعرف"
                  }
                }
              ],
              "gate": {
                "all": [
                  {
                    "field": "QP01_Participation",
                    "op": "in",
                    "values": [
                      1
                    ]
                  },
                  {
                    "all": [
                      {
                        "field": "QP01_Produit",
                        "op": "not_in",
                        "values": [
                          0
                        ]
                      },
                      {
                        "field": "QP03_Situation",
                        "op": "in",
                        "values": [
                          1,
                          2
                        ]
                      }
                    ]
                  }
                ]
              },
              "source": "Rapport méthodologique, annexe 1bis, pp. 23–27, QP03"
            },
            {
              "id": "QP03_Demandeur_Autre",
              "label": {
                "fr": "Autre : précisez",
                "en": "Other: please specify",
                "es": "Otra : especifique",
                "pt": "Outra : especifique",
                "ar": "أخرى : يرجى التحديد"
              },
              "type": "text",
              "options": [],
              "max_chars": 500,
              "gate": {
                "all": [
                  {
                    "field": "QP01_Participation",
                    "op": "in",
                    "values": [
                      1
                    ]
                  },
                  {
                    "all": [
                      {
                        "field": "QP01_Produit",
                        "op": "not_in",
                        "values": [
                          0
                        ]
                      },
                      {
                        "field": "QP03_Demandeur",
                        "op": "in",
                        "values": [
                          97
                        ]
                      }
                    ]
                  }
                ]
              },
              "source": "Rapport méthodologique, annexe 1bis, pp. 23–27, QP03"
            },
            {
              "id": "QP03_Circuit",
              "label": {
                "fr": "Circuit principal",
                "en": "Main request pathway",
                "es": "Circuito principal de la solicitud",
                "pt": "Circuito principal do pedido",
                "ar": "المسار الرئيسي للطلب"
              },
              "type": "single",
              "options": [
                {
                  "code": 1,
                  "label": {
                    "fr": "Demande reçue directement de l’utilisateur",
                    "en": "Request received directly from the user",
                    "es": "Solicitud recibida directamente del usuario",
                    "pt": "Pedido recebido diretamente do utilizador",
                    "ar": "طلب استلم مباشرة من المستخدم"
                  }
                },
                {
                  "code": 2,
                  "label": {
                    "fr": "Demande transmise par l’INS",
                    "en": "Request forwarded by the NSO",
                    "es": "Solicitud transmitida por la ONE",
                    "pt": "Pedido transmitido pelo INE",
                    "ar": "طلب أحاله الجهاز الوطني للإحصاء"
                  }
                },
                {
                  "code": 3,
                  "label": {
                    "fr": "Demande transmise par un autre producteur ou service",
                    "en": "Request forwarded by another producer or service",
                    "es": "Solicitud transmitida por otro productor o servicio",
                    "pt": "Pedido transmitido por outro produtor ou serviço",
                    "ar": "طلب أحاله منتج أو إدارة أخرى"
                  }
                },
                {
                  "code": 97,
                  "label": {
                    "fr": "Autre",
                    "en": "Other",
                    "es": "Otra",
                    "pt": "Outra",
                    "ar": "أخرى"
                  }
                },
                {
                  "code": 98,
                  "label": {
                    "fr": "Ne sait pas",
                    "en": "Do not know",
                    "es": "No sabe",
                    "pt": "Não sabe",
                    "ar": "لا أعرف"
                  }
                }
              ],
              "gate": {
                "all": [
                  {
                    "field": "QP01_Participation",
                    "op": "in",
                    "values": [
                      1
                    ]
                  },
                  {
                    "all": [
                      {
                        "field": "QP01_Produit",
                        "op": "not_in",
                        "values": [
                          0
                        ]
                      },
                      {
                        "field": "QP03_Situation",
                        "op": "in",
                        "values": [
                          1,
                          2
                        ]
                      }
                    ]
                  }
                ]
              },
              "source": "Rapport méthodologique, annexe 1bis, pp. 23–27, QP03"
            },
            {
              "id": "QP03_Circuit_Autre",
              "label": {
                "fr": "Autre : précisez",
                "en": "Other: please specify",
                "es": "Otra : especifique",
                "pt": "Outra : especifique",
                "ar": "أخرى : يرجى التحديد"
              },
              "type": "text",
              "options": [],
              "max_chars": 500,
              "gate": {
                "all": [
                  {
                    "field": "QP01_Participation",
                    "op": "in",
                    "values": [
                      1
                    ]
                  },
                  {
                    "all": [
                      {
                        "field": "QP01_Produit",
                        "op": "not_in",
                        "values": [
                          0
                        ]
                      },
                      {
                        "field": "QP03_Circuit",
                        "op": "in",
                        "values": [
                          97
                        ]
                      }
                    ]
                  }
                ]
              },
              "source": "Rapport méthodologique, annexe 1bis, pp. 23–27, QP03"
            },
            {
              "id": "QP03_Canal",
              "label": {
                "fr": "Canal principal de réception",
                "en": "Main channel through which the request was received",
                "es": "Canal principal de recepción de la solicitud",
                "pt": "Principal canal de receção do pedido",
                "ar": "القناة الرئيسية لاستلام الطلب"
              },
              "type": "single",
              "options": [
                {
                  "code": 1,
                  "label": {
                    "fr": "Courriel",
                    "en": "Email",
                    "es": "Correo electrónico",
                    "pt": "Correio eletrónico",
                    "ar": "البريد الإلكتروني"
                  }
                },
                {
                  "code": 2,
                  "label": {
                    "fr": "Courrier ou note officielle",
                    "en": "Letter or official memorandum",
                    "es": "Carta o nota oficial",
                    "pt": "Carta ou nota oficial",
                    "ar": "رسالة أو مذكرة رسمية"
                  }
                },
                {
                  "code": 3,
                  "label": {
                    "fr": "Téléphone ou échange oral",
                    "en": "Telephone or oral exchange",
                    "es": "Teléfono o intercambio oral",
                    "pt": "Telefone ou troca oral",
                    "ar": "الهاتف أو التواصل الشفهي"
                  }
                },
                {
                  "code": 4,
                  "label": {
                    "fr": "Messagerie instantanée",
                    "en": "Instant messaging",
                    "es": "Mensajería instantánea",
                    "pt": "Mensagens instantâneas",
                    "ar": "المراسلة الفورية"
                  }
                },
                {
                  "code": 5,
                  "label": {
                    "fr": "Formulaire ou plateforme",
                    "en": "Form or platform",
                    "es": "Formulario o plataforma",
                    "pt": "Formulário ou plataforma",
                    "ar": "استمارة أو منصة"
                  }
                },
                {
                  "code": 97,
                  "label": {
                    "fr": "Autre",
                    "en": "Other",
                    "es": "Otra",
                    "pt": "Outra",
                    "ar": "أخرى"
                  }
                },
                {
                  "code": 98,
                  "label": {
                    "fr": "Ne sait pas",
                    "en": "Do not know",
                    "es": "No sabe",
                    "pt": "Não sabe",
                    "ar": "لا أعرف"
                  }
                }
              ],
              "gate": {
                "all": [
                  {
                    "field": "QP01_Participation",
                    "op": "in",
                    "values": [
                      1
                    ]
                  },
                  {
                    "all": [
                      {
                        "field": "QP01_Produit",
                        "op": "not_in",
                        "values": [
                          0
                        ]
                      },
                      {
                        "field": "QP03_Situation",
                        "op": "in",
                        "values": [
                          1,
                          2
                        ]
                      }
                    ]
                  }
                ]
              },
              "source": "Rapport méthodologique, annexe 1bis, pp. 23–27, QP03"
            },
            {
              "id": "QP03_Canal_Autre",
              "label": {
                "fr": "Autre : précisez",
                "en": "Other: please specify",
                "es": "Otra : especifique",
                "pt": "Outra : especifique",
                "ar": "أخرى : يرجى التحديد"
              },
              "type": "text",
              "options": [],
              "max_chars": 500,
              "gate": {
                "all": [
                  {
                    "field": "QP01_Participation",
                    "op": "in",
                    "values": [
                      1
                    ]
                  },
                  {
                    "all": [
                      {
                        "field": "QP01_Produit",
                        "op": "not_in",
                        "values": [
                          0
                        ]
                      },
                      {
                        "field": "QP03_Canal",
                        "op": "in",
                        "values": [
                          97
                        ]
                      }
                    ]
                  }
                ]
              },
              "source": "Rapport méthodologique, annexe 1bis, pp. 23–27, QP03"
            }
          ],
          "note": {
            "fr": "Choisir la demande la plus récemment clôturée pendant la période de référence, qu’elle ait ou non abouti à la fourniture de chiffres. Ne pas sélectionner uniquement un exemple particulièrement réussi. Les situations 3 et 98 conduisent à QP08. La situation 2 conduit à QP04, puis QP08.",
            "en": "Select the request most recently closed during the reference period, whether or not figures were provided. Do not select only a particularly successful example. Situations 3 and 98 lead to QP08. Situation 2 leads to QP04 and then QP08.",
            "es": "Seleccione la solicitud cerrada más recientemente durante el período, se hayan facilitado cifras o no. No elija solo un caso exitoso. Las situaciones 3 y 98 llevan a QP08. La situación 2 lleva a QP04 y después a QP08.",
            "pt": "Selecione o pedido encerrado mais recentemente no período, tenham sido fornecidos números ou não. Não escolha apenas um caso bem-sucedido. As situações 3 e 98 levam a QP08. A situação 2 leva a QP04 e depois a QP08.",
            "ar": "اختر أحدث طلب أغلق خلال الفترة، سواء قدمت أرقام أم لا. لا تختَر حالة ناجحة فقط. تؤدي الحالتان 3 و98 إلى QP08، وتؤدي الحالة 2 إلى QP04 ثم QP08."
          },
          "gate": {
            "field": "QP02a",
            "op": "not_in",
            "values": [
              0
            ]
          },
          "theme": "Demande et urgence",
          "source": "Rapport méthodologique, annexe 1bis, pp. 23–27, QP03"
        },
        {
          "id": "QP04",
          "title": {
            "fr": "Délai et traitement de cette dernière demande",
            "en": "Deadline and handling of this most recent request",
            "es": "Plazo y tratamiento de esta última solicitud",
            "pt": "Prazo e tratamento deste último pedido",
            "ar": "مهلة هذا الطلب الأخير ومعالجته"
          },
          "fields": [
            {
              "id": "QP04a",
              "label": {
                "fr": "Quel délai était laissé à votre organisme pour répondre, à partir de la réception de la demande ?",
                "en": "How much time was your organisation given to respond from receipt of the request?",
                "es": "¿Cuánto tiempo se concedió a su organismo para responder desde la recepción de la solicitud?",
                "pt": "Quanto tempo foi concedido ao seu organismo para responder a partir da receção do pedido?",
                "ar": "ما المهلة التي أتيحت لمؤسستكم للإجابة منذ استلام الطلب؟"
              },
              "type": "single",
              "options": [
                {
                  "code": 1,
                  "label": {
                    "fr": "Moins de 6 heures",
                    "en": "Less than 6 hours",
                    "es": "Menos de 6 horas",
                    "pt": "Menos de 6 horas",
                    "ar": "أقل من 6 ساعات"
                  }
                },
                {
                  "code": 2,
                  "label": {
                    "fr": "De 6 à 24 heures incluses",
                    "en": "6 to 24 hours inclusive",
                    "es": "De 6 a 24 horas, inclusive",
                    "pt": "De 6 a 24 horas, inclusive",
                    "ar": "من 6 إلى 24 ساعة شاملة"
                  }
                },
                {
                  "code": 3,
                  "label": {
                    "fr": "Plus de 24 heures à 3 jours inclus",
                    "en": "More than 24 hours up to 3 days inclusive",
                    "es": "Más de 24 horas y hasta 3 días, inclusive",
                    "pt": "Mais de 24 horas até 3 dias, inclusive",
                    "ar": "أكثر من 24 ساعة وحتى 3 أيام شاملة"
                  }
                },
                {
                  "code": 4,
                  "label": {
                    "fr": "Plus de 3 jours",
                    "en": "More than 3 days",
                    "es": "Más de 3 días",
                    "pt": "Mais de 3 dias",
                    "ar": "أكثر من 3 أيام"
                  }
                },
                {
                  "code": 5,
                  "label": {
                    "fr": "Aucune échéance indiquée",
                    "en": "No deadline specified",
                    "es": "Sin plazo especificado",
                    "pt": "Sem prazo indicado",
                    "ar": "لم تحدد مهلة"
                  }
                },
                {
                  "code": 98,
                  "label": {
                    "fr": "Ne sait pas",
                    "en": "Do not know",
                    "es": "No sabe",
                    "pt": "Não sabe",
                    "ar": "لا أعرف"
                  }
                }
              ],
              "gate": {
                "all": [
                  {
                    "field": "QP01_Participation",
                    "op": "in",
                    "values": [
                      1
                    ]
                  },
                  {
                    "field": "QP01_Produit",
                    "op": "not_in",
                    "values": [
                      0
                    ]
                  }
                ]
              },
              "source": "Rapport méthodologique, annexe 1bis, pp. 23–27, QP04"
            },
            {
              "id": "QP04b",
              "label": {
                "fr": "La réponse de votre organisme a-t-elle été transmise avant l’échéance demandée ?",
                "en": "Was your organisation’s response sent before the requested deadline?",
                "es": "¿Se envió la respuesta de su organismo antes del plazo solicitado?",
                "pt": "A resposta do seu organismo foi enviada antes do prazo solicitado?",
                "ar": "هل أرسلت إجابة مؤسستكم قبل الموعد المطلوب؟"
              },
              "type": "single",
              "options": [
                {
                  "code": 1,
                  "label": {
                    "fr": "Oui",
                    "en": "Yes",
                    "es": "Sí",
                    "pt": "Sim",
                    "ar": "نعم"
                  }
                },
                {
                  "code": 0,
                  "label": {
                    "fr": "Non",
                    "en": "No",
                    "es": "No",
                    "pt": "Não",
                    "ar": "لا"
                  }
                },
                {
                  "code": 98,
                  "label": {
                    "fr": "Ne sait pas",
                    "en": "Do not know",
                    "es": "No sabe",
                    "pt": "Não sabe",
                    "ar": "لا أعرف"
                  }
                },
                {
                  "code": 99,
                  "label": {
                    "fr": "Non applicable, aucune échéance indiquée",
                    "en": "Not applicable, no deadline specified",
                    "es": "No aplicable, sin plazo especificado",
                    "pt": "Não aplicável, sem prazo indicado",
                    "ar": "لا ينطبق، لم تحدد مهلة"
                  }
                }
              ],
              "gate": {
                "all": [
                  {
                    "field": "QP01_Participation",
                    "op": "in",
                    "values": [
                      1
                    ]
                  },
                  {
                    "field": "QP01_Produit",
                    "op": "not_in",
                    "values": [
                      0
                    ]
                  }
                ]
              },
              "source": "Rapport méthodologique, annexe 1bis, pp. 23–27, QP04"
            },
            {
              "id": "QP04c",
              "label": {
                "fr": "Si aucun chiffre n’a été fourni ou si le délai n’a pas été respecté, quel a été le principal obstacle ?",
                "en": "If no figures were provided or the deadline was missed, what was the main obstacle?",
                "es": "Si no se proporcionaron cifras o se incumplió el plazo, ¿cuál fue el principal obstáculo?",
                "pt": "Se não foram fornecidos números ou se o prazo não foi cumprido, qual foi o principal obstáculo?",
                "ar": "إذا لم تقدم أرقام أو لم تحترم المهلة، فما العائق الرئيسي؟"
              },
              "type": "single",
              "options": [
                {
                  "code": 1,
                  "label": {
                    "fr": "Donnée appropriée indisponible",
                    "en": "Suitable data unavailable",
                    "es": "Datos adecuados no disponibles",
                    "pt": "Dados adequados indisponíveis",
                    "ar": "عدم توفر بيانات مناسبة"
                  }
                },
                {
                  "code": 2,
                  "label": {
                    "fr": "Besoin insuffisamment précisé",
                    "en": "Need insufficiently specified",
                    "es": "Necesidad insuficientemente precisada",
                    "pt": "Necessidade insuficientemente especificada",
                    "ar": "عدم تحديد الحاجة بما يكفي"
                  }
                },
                {
                  "code": 3,
                  "label": {
                    "fr": "Délai trop court",
                    "en": "Deadline too short",
                    "es": "Plazo demasiado corto",
                    "pt": "Prazo demasiado curto",
                    "ar": "قصر المهلة الشديد"
                  }
                },
                {
                  "code": 4,
                  "label": {
                    "fr": "Contrôle ou validation inachevé",
                    "en": "Checking or validation incomplete",
                    "es": "Control o validación incompletos",
                    "pt": "Controlo ou validação incompletos",
                    "ar": "المراجعة أو التحقق غير مكتملين"
                  }
                },
                {
                  "code": 5,
                  "label": {
                    "fr": "Agent compétent indisponible",
                    "en": "Responsible staff member unavailable",
                    "es": "Agente competente no disponible",
                    "pt": "Agente competente indisponível",
                    "ar": "عدم توفر الموظف المختص"
                  }
                },
                {
                  "code": 6,
                  "label": {
                    "fr": "Restrictions de diffusion ou d’accès",
                    "en": "Dissemination or access restrictions",
                    "es": "Restricciones de difusión o acceso",
                    "pt": "Restrições de divulgação ou acesso",
                    "ar": "قيود النشر أو الوصول"
                  }
                },
                {
                  "code": 7,
                  "label": {
                    "fr": "Divergence entre sources non résolue",
                    "en": "Unresolved discrepancy between sources",
                    "es": "Divergencia entre fuentes sin resolver",
                    "pt": "Divergência entre fontes não resolvida",
                    "ar": "اختلاف بين المصادر لم يحسم"
                  }
                },
                {
                  "code": 97,
                  "label": {
                    "fr": "Autre",
                    "en": "Other",
                    "es": "Otra",
                    "pt": "Outra",
                    "ar": "أخرى"
                  }
                },
                {
                  "code": 98,
                  "label": {
                    "fr": "Ne sait pas",
                    "en": "Do not know",
                    "es": "No sabe",
                    "pt": "Não sabe",
                    "ar": "لا أعرف"
                  }
                },
                {
                  "code": 99,
                  "label": {
                    "fr": "Non applicable",
                    "en": "Not applicable",
                    "es": "No aplicable",
                    "pt": "Não aplicável",
                    "ar": "لا ينطبق"
                  }
                }
              ],
              "gate": {
                "all": [
                  {
                    "field": "QP01_Participation",
                    "op": "in",
                    "values": [
                      1
                    ]
                  },
                  {
                    "field": "QP01_Produit",
                    "op": "not_in",
                    "values": [
                      0
                    ]
                  }
                ]
              },
              "source": "Rapport méthodologique, annexe 1bis, pp. 23–27, QP04"
            },
            {
              "id": "QP04c_Autre",
              "label": {
                "fr": "Autre : précisez",
                "en": "Other: please specify",
                "es": "Otra : especifique",
                "pt": "Outra : especifique",
                "ar": "أخرى : يرجى التحديد"
              },
              "type": "text",
              "options": [],
              "max_chars": 500,
              "gate": {
                "all": [
                  {
                    "field": "QP01_Participation",
                    "op": "in",
                    "values": [
                      1
                    ]
                  },
                  {
                    "all": [
                      {
                        "field": "QP01_Produit",
                        "op": "not_in",
                        "values": [
                          0
                        ]
                      },
                      {
                        "field": "QP04c",
                        "op": "in",
                        "values": [
                          97
                        ]
                      }
                    ]
                  }
                ]
              },
              "source": "Rapport méthodologique, annexe 1bis, pp. 23–27, QP04"
            }
          ],
          "note": {
            "fr": "Les durées sont calendaires, y compris les nuits et week-ends. Une réponse expliquant qu’aucun chiffre ne pouvait être fourni reste distinguée de la fourniture de chiffres par QP03. Le respect d’un délai ne suffit pas à conclure que le besoin statistique a été satisfait.",
            "en": "Times are calendar durations, including nights and weekends. QP03 distinguishes a response explaining that no figures could be provided from an actual provision of figures. Meeting a deadline does not by itself establish that the statistical need was met.",
            "es": "Los plazos son naturales e incluyen noches y fines de semana. QP03 distingue la imposibilidad de facilitar cifras del suministro efectivo. Cumplir un plazo no demuestra por sí solo que se haya satisfecho la necesidad estadística.",
            "pt": "Os prazos são de calendário, incluindo noites e fins de semana. QP03 distingue a impossibilidade de fornecer números do fornecimento efetivo. Cumprir um prazo não demonstra, por si só, que a necessidade estatística foi satisfeita.",
            "ar": "المهل تقويمية وتشمل الليالي وعطلات نهاية الأسبوع. يميز QP03 بين تعذر تقديم أرقام وتقديمها فعليا. لا يثبت احترام المهلة وحده تلبية الحاجة الإحصائية."
          },
          "gate": {
            "all": [
              {
                "field": "QP02a",
                "op": "not_in",
                "values": [
                  0
                ]
              },
              {
                "field": "QP03_Situation",
                "op": "in",
                "values": [
                  1,
                  2
                ]
              }
            ]
          },
          "theme": "Demande et urgence",
          "source": "Rapport méthodologique, annexe 1bis, pp. 23–27, QP04"
        },
        {
          "id": "QP05",
          "title": {
            "fr": "Origine et documentation du premier chiffre transmis",
            "en": "Origin and documentation of the first figure provided",
            "es": "Origen y documentación de la primera cifra facilitada",
            "pt": "Origem e documentação do primeiro número fornecido",
            "ar": "مصدر أول رقم مقدم وتوثيقه"
          },
          "fields": [
            {
              "id": "QP05_Origine",
              "label": {
                "fr": "Origine principale de ce chiffre",
                "en": "Main origin of this figure",
                "es": "Origen principal de esta cifra",
                "pt": "Origem principal deste número",
                "ar": "المصدر الرئيسي لهذا الرقم"
              },
              "type": "single",
              "options": [
                {
                  "code": 1,
                  "label": {
                    "fr": "Enquête ou recensement produit par votre organisme",
                    "en": "Survey or census produced by your organisation",
                    "es": "Encuesta o censo producido por su organismo",
                    "pt": "Inquérito ou recenseamento produzido pelo seu organismo",
                    "ar": "مسح أو تعداد أنتجته مؤسستكم"
                  }
                },
                {
                  "code": 2,
                  "label": {
                    "fr": "Données administratives traitées par votre organisme",
                    "en": "Administrative data processed by your organisation",
                    "es": "Datos administrativos tratados por su organismo",
                    "pt": "Dados administrativos tratados pelo seu organismo",
                    "ar": "بيانات إدارية عالجتها مؤسستكم"
                  }
                },
                {
                  "code": 3,
                  "label": {
                    "fr": "Autre résultat statistique produit par votre organisme",
                    "en": "Other statistical result produced by your organisation",
                    "es": "Otro resultado estadístico producido por su organismo",
                    "pt": "Outro resultado estatístico produzido pelo seu organismo",
                    "ar": "نتيجة إحصائية أخرى أنتجتها مؤسستكم"
                  }
                },
                {
                  "code": 4,
                  "label": {
                    "fr": "Publication de l’INS",
                    "en": "NSO publication",
                    "es": "Publicación de la ONE",
                    "pt": "Publicação do INE",
                    "ar": "منشور الجهاز الوطني للإحصاء"
                  }
                },
                {
                  "code": 5,
                  "label": {
                    "fr": "Autre administration nationale",
                    "en": "Other national administration",
                    "es": "Otra administración nacional",
                    "pt": "Outra administração nacional",
                    "ar": "إدارة وطنية أخرى"
                  }
                },
                {
                  "code": 6,
                  "label": {
                    "fr": "Organisation régionale ou internationale",
                    "en": "Regional or international organisation",
                    "es": "Organización regional o internacional",
                    "pt": "Organização regional ou internacional",
                    "ar": "منظمة إقليمية أو دولية"
                  }
                },
                {
                  "code": 7,
                  "label": {
                    "fr": "Recherche ou source privée identifiée",
                    "en": "Identified research or private source",
                    "es": "Investigación o fuente privada identificada",
                    "pt": "Investigação ou fonte privada identificada",
                    "ar": "بحث أو مصدر خاص محدد"
                  }
                },
                {
                  "code": 8,
                  "label": {
                    "fr": "Document antérieur sans retour à la source statistique",
                    "en": "Earlier document without consulting the statistical source",
                    "es": "Documento anterior sin consultar la fuente estadística",
                    "pt": "Documento anterior sem consultar a fonte estatística",
                    "ar": "وثيقة سابقة دون الرجوع إلى المصدر الإحصائي"
                  }
                },
                {
                  "code": 9,
                  "label": {
                    "fr": "Source non identifiée",
                    "en": "Unidentified source",
                    "es": "Fuente no identificada",
                    "pt": "Fonte não identificada",
                    "ar": "مصدر غير محدد"
                  }
                },
                {
                  "code": 97,
                  "label": {
                    "fr": "Autre",
                    "en": "Other",
                    "es": "Otra",
                    "pt": "Outra",
                    "ar": "أخرى"
                  }
                },
                {
                  "code": 98,
                  "label": {
                    "fr": "Ne sait pas",
                    "en": "Do not know",
                    "es": "No sabe",
                    "pt": "Não sabe",
                    "ar": "لا أعرف"
                  }
                }
              ],
              "gate": {
                "all": [
                  {
                    "field": "QP01_Participation",
                    "op": "in",
                    "values": [
                      1
                    ]
                  },
                  {
                    "field": "QP01_Produit",
                    "op": "not_in",
                    "values": [
                      0
                    ]
                  }
                ]
              },
              "source": "Rapport méthodologique, annexe 1bis, pp. 23–27, QP05"
            },
            {
              "id": "QP05_Origine_Autre",
              "label": {
                "fr": "Autre : précisez",
                "en": "Other: please specify",
                "es": "Otra : especifique",
                "pt": "Outra : especifique",
                "ar": "أخرى : يرجى التحديد"
              },
              "type": "text",
              "options": [],
              "max_chars": 500,
              "gate": {
                "all": [
                  {
                    "field": "QP01_Participation",
                    "op": "in",
                    "values": [
                      1
                    ]
                  },
                  {
                    "all": [
                      {
                        "field": "QP01_Produit",
                        "op": "not_in",
                        "values": [
                          0
                        ]
                      },
                      {
                        "field": "QP05_Origine",
                        "op": "in",
                        "values": [
                          97
                        ]
                      }
                    ]
                  }
                ]
              },
              "source": "Rapport méthodologique, annexe 1bis, pp. 23–27, QP05"
            },
            {
              "id": "QP05_S1",
              "label": {
                "fr": "Source précise permettant de retrouver le résultat",
                "en": "Specific source enabling the result to be located",
                "es": "Fuente precisa que permite localizar el resultado",
                "pt": "Fonte precisa que permite localizar o resultado",
                "ar": "مصدر دقيق يتيح العثور على النتيجة"
              },
              "type": "single",
              "options": [
                {
                  "code": 1,
                  "label": {
                    "fr": "Présente dans la réponse",
                    "en": "Included in the response",
                    "es": "Incluida en la respuesta",
                    "pt": "Presente na resposta",
                    "ar": "واردة في الإجابة"
                  }
                },
                {
                  "code": 2,
                  "label": {
                    "fr": "Accessible par un lien ou une référence précise transmis avec la réponse",
                    "en": "Available through a specific link or reference sent with the response",
                    "es": "Accesible mediante un enlace o referencia precisa enviados con la respuesta",
                    "pt": "Acessível por uma ligação ou referência precisa enviada com a resposta",
                    "ar": "متاحة عبر رابط أو مرجع دقيق أرفق بالإجابة"
                  }
                },
                {
                  "code": 0,
                  "label": {
                    "fr": "Absente",
                    "en": "Absent",
                    "es": "Ausente",
                    "pt": "Ausente",
                    "ar": "غير موجودة"
                  }
                },
                {
                  "code": 98,
                  "label": {
                    "fr": "Ne sait pas",
                    "en": "Do not know",
                    "es": "No sabe",
                    "pt": "Não sabe",
                    "ar": "لا أعرف"
                  }
                },
                {
                  "code": 99,
                  "label": {
                    "fr": "Non applicable",
                    "en": "Not applicable",
                    "es": "No aplicable",
                    "pt": "Não aplicável",
                    "ar": "لا ينطبق"
                  }
                }
              ],
              "gate": {
                "all": [
                  {
                    "field": "QP01_Participation",
                    "op": "in",
                    "values": [
                      1
                    ]
                  },
                  {
                    "field": "QP01_Produit",
                    "op": "not_in",
                    "values": [
                      0
                    ]
                  }
                ]
              },
              "source": "Rapport méthodologique, annexe 1bis, pp. 23–27, QP05"
            },
            {
              "id": "QP05_S2",
              "label": {
                "fr": "Date de publication ou version de la source",
                "en": "Publication date or source version",
                "es": "Fecha de publicación o versión de la fuente",
                "pt": "Data de publicação ou versão da fonte",
                "ar": "تاريخ النشر أو نسخة المصدر"
              },
              "type": "single",
              "options": [
                {
                  "code": 1,
                  "label": {
                    "fr": "Présente dans la réponse",
                    "en": "Included in the response",
                    "es": "Incluida en la respuesta",
                    "pt": "Presente na resposta",
                    "ar": "واردة في الإجابة"
                  }
                },
                {
                  "code": 2,
                  "label": {
                    "fr": "Accessible par un lien ou une référence précise transmis avec la réponse",
                    "en": "Available through a specific link or reference sent with the response",
                    "es": "Accesible mediante un enlace o referencia precisa enviados con la respuesta",
                    "pt": "Acessível por uma ligação ou referência precisa enviada com a resposta",
                    "ar": "متاحة عبر رابط أو مرجع دقيق أرفق بالإجابة"
                  }
                },
                {
                  "code": 0,
                  "label": {
                    "fr": "Absente",
                    "en": "Absent",
                    "es": "Ausente",
                    "pt": "Ausente",
                    "ar": "غير موجودة"
                  }
                },
                {
                  "code": 98,
                  "label": {
                    "fr": "Ne sait pas",
                    "en": "Do not know",
                    "es": "No sabe",
                    "pt": "Não sabe",
                    "ar": "لا أعرف"
                  }
                },
                {
                  "code": 99,
                  "label": {
                    "fr": "Non applicable",
                    "en": "Not applicable",
                    "es": "No aplicable",
                    "pt": "Não aplicável",
                    "ar": "لا ينطبق"
                  }
                }
              ],
              "gate": {
                "all": [
                  {
                    "field": "QP01_Participation",
                    "op": "in",
                    "values": [
                      1
                    ]
                  },
                  {
                    "field": "QP01_Produit",
                    "op": "not_in",
                    "values": [
                      0
                    ]
                  }
                ]
              },
              "source": "Rapport méthodologique, annexe 1bis, pp. 23–27, QP05"
            },
            {
              "id": "QP05_S3",
              "label": {
                "fr": "Période à laquelle se rapporte la valeur",
                "en": "Period to which the value relates",
                "es": "Período al que se refiere el valor",
                "pt": "Período a que se refere o valor",
                "ar": "الفترة التي تتعلق بها القيمة"
              },
              "type": "single",
              "options": [
                {
                  "code": 1,
                  "label": {
                    "fr": "Présente dans la réponse",
                    "en": "Included in the response",
                    "es": "Incluida en la respuesta",
                    "pt": "Presente na resposta",
                    "ar": "واردة في الإجابة"
                  }
                },
                {
                  "code": 2,
                  "label": {
                    "fr": "Accessible par un lien ou une référence précise transmis avec la réponse",
                    "en": "Available through a specific link or reference sent with the response",
                    "es": "Accesible mediante un enlace o referencia precisa enviados con la respuesta",
                    "pt": "Acessível por uma ligação ou referência precisa enviada com a resposta",
                    "ar": "متاحة عبر رابط أو مرجع دقيق أرفق بالإجابة"
                  }
                },
                {
                  "code": 0,
                  "label": {
                    "fr": "Absente",
                    "en": "Absent",
                    "es": "Ausente",
                    "pt": "Ausente",
                    "ar": "غير موجودة"
                  }
                },
                {
                  "code": 98,
                  "label": {
                    "fr": "Ne sait pas",
                    "en": "Do not know",
                    "es": "No sabe",
                    "pt": "Não sabe",
                    "ar": "لا أعرف"
                  }
                },
                {
                  "code": 99,
                  "label": {
                    "fr": "Non applicable",
                    "en": "Not applicable",
                    "es": "No aplicable",
                    "pt": "Não aplicável",
                    "ar": "لا ينطبق"
                  }
                }
              ],
              "gate": {
                "all": [
                  {
                    "field": "QP01_Participation",
                    "op": "in",
                    "values": [
                      1
                    ]
                  },
                  {
                    "field": "QP01_Produit",
                    "op": "not_in",
                    "values": [
                      0
                    ]
                  }
                ]
              },
              "source": "Rapport méthodologique, annexe 1bis, pp. 23–27, QP05"
            },
            {
              "id": "QP05_S4",
              "label": {
                "fr": "Définition de l’indicateur",
                "en": "Definition of the indicator",
                "es": "Definición del indicador",
                "pt": "Definição do indicador",
                "ar": "تعريف المؤشر"
              },
              "type": "single",
              "options": [
                {
                  "code": 1,
                  "label": {
                    "fr": "Présente dans la réponse",
                    "en": "Included in the response",
                    "es": "Incluida en la respuesta",
                    "pt": "Presente na resposta",
                    "ar": "واردة في الإجابة"
                  }
                },
                {
                  "code": 2,
                  "label": {
                    "fr": "Accessible par un lien ou une référence précise transmis avec la réponse",
                    "en": "Available through a specific link or reference sent with the response",
                    "es": "Accesible mediante un enlace o referencia precisa enviados con la respuesta",
                    "pt": "Acessível por uma ligação ou referência precisa enviada com a resposta",
                    "ar": "متاحة عبر رابط أو مرجع دقيق أرفق بالإجابة"
                  }
                },
                {
                  "code": 0,
                  "label": {
                    "fr": "Absente",
                    "en": "Absent",
                    "es": "Ausente",
                    "pt": "Ausente",
                    "ar": "غير موجودة"
                  }
                },
                {
                  "code": 98,
                  "label": {
                    "fr": "Ne sait pas",
                    "en": "Do not know",
                    "es": "No sabe",
                    "pt": "Não sabe",
                    "ar": "لا أعرف"
                  }
                },
                {
                  "code": 99,
                  "label": {
                    "fr": "Non applicable",
                    "en": "Not applicable",
                    "es": "No aplicable",
                    "pt": "Não aplicável",
                    "ar": "لا ينطبق"
                  }
                }
              ],
              "gate": {
                "all": [
                  {
                    "field": "QP01_Participation",
                    "op": "in",
                    "values": [
                      1
                    ]
                  },
                  {
                    "field": "QP01_Produit",
                    "op": "not_in",
                    "values": [
                      0
                    ]
                  }
                ]
              },
              "source": "Rapport méthodologique, annexe 1bis, pp. 23–27, QP05"
            },
            {
              "id": "QP05_S5",
              "label": {
                "fr": "Unité et base de calcul, notamment dénominateur d’un taux",
                "en": "Unit and calculation basis, particularly the denominator of a rate",
                "es": "Unidad y base de cálculo, en particular el denominador de una tasa",
                "pt": "Unidade e base de cálculo, nomeadamente o denominador de uma taxa",
                "ar": "الوحدة وأساس الحساب، ولا سيما مقام المعدل"
              },
              "type": "single",
              "options": [
                {
                  "code": 1,
                  "label": {
                    "fr": "Présente dans la réponse",
                    "en": "Included in the response",
                    "es": "Incluida en la respuesta",
                    "pt": "Presente na resposta",
                    "ar": "واردة في الإجابة"
                  }
                },
                {
                  "code": 2,
                  "label": {
                    "fr": "Accessible par un lien ou une référence précise transmis avec la réponse",
                    "en": "Available through a specific link or reference sent with the response",
                    "es": "Accesible mediante un enlace o referencia precisa enviados con la respuesta",
                    "pt": "Acessível por uma ligação ou referência precisa enviada com a resposta",
                    "ar": "متاحة عبر رابط أو مرجع دقيق أرفق بالإجابة"
                  }
                },
                {
                  "code": 0,
                  "label": {
                    "fr": "Absente",
                    "en": "Absent",
                    "es": "Ausente",
                    "pt": "Ausente",
                    "ar": "غير موجودة"
                  }
                },
                {
                  "code": 98,
                  "label": {
                    "fr": "Ne sait pas",
                    "en": "Do not know",
                    "es": "No sabe",
                    "pt": "Não sabe",
                    "ar": "لا أعرف"
                  }
                },
                {
                  "code": 99,
                  "label": {
                    "fr": "Non applicable",
                    "en": "Not applicable",
                    "es": "No aplicable",
                    "pt": "Não aplicável",
                    "ar": "لا ينطبق"
                  }
                }
              ],
              "gate": {
                "all": [
                  {
                    "field": "QP01_Participation",
                    "op": "in",
                    "values": [
                      1
                    ]
                  },
                  {
                    "field": "QP01_Produit",
                    "op": "not_in",
                    "values": [
                      0
                    ]
                  }
                ]
              },
              "source": "Rapport méthodologique, annexe 1bis, pp. 23–27, QP05"
            },
            {
              "id": "QP05_S6",
              "label": {
                "fr": "Population et territoire couverts",
                "en": "Population and geographical coverage",
                "es": "Población y territorio cubiertos",
                "pt": "População e território abrangidos",
                "ar": "السكان والنطاق الجغرافي المشمولان"
              },
              "type": "single",
              "options": [
                {
                  "code": 1,
                  "label": {
                    "fr": "Présente dans la réponse",
                    "en": "Included in the response",
                    "es": "Incluida en la respuesta",
                    "pt": "Presente na resposta",
                    "ar": "واردة في الإجابة"
                  }
                },
                {
                  "code": 2,
                  "label": {
                    "fr": "Accessible par un lien ou une référence précise transmis avec la réponse",
                    "en": "Available through a specific link or reference sent with the response",
                    "es": "Accesible mediante un enlace o referencia precisa enviados con la respuesta",
                    "pt": "Acessível por uma ligação ou referência precisa enviada com a resposta",
                    "ar": "متاحة عبر رابط أو مرجع دقيق أرفق بالإجابة"
                  }
                },
                {
                  "code": 0,
                  "label": {
                    "fr": "Absente",
                    "en": "Absent",
                    "es": "Ausente",
                    "pt": "Ausente",
                    "ar": "غير موجودة"
                  }
                },
                {
                  "code": 98,
                  "label": {
                    "fr": "Ne sait pas",
                    "en": "Do not know",
                    "es": "No sabe",
                    "pt": "Não sabe",
                    "ar": "لا أعرف"
                  }
                },
                {
                  "code": 99,
                  "label": {
                    "fr": "Non applicable",
                    "en": "Not applicable",
                    "es": "No aplicable",
                    "pt": "Não aplicável",
                    "ar": "لا ينطبق"
                  }
                }
              ],
              "gate": {
                "all": [
                  {
                    "field": "QP01_Participation",
                    "op": "in",
                    "values": [
                      1
                    ]
                  },
                  {
                    "field": "QP01_Produit",
                    "op": "not_in",
                    "values": [
                      0
                    ]
                  }
                ]
              },
              "source": "Rapport méthodologique, annexe 1bis, pp. 23–27, QP05"
            },
            {
              "id": "QP05_S7",
              "label": {
                "fr": "Limites utiles à l’interprétation ou à la comparaison",
                "en": "Limitations relevant to interpretation or comparison",
                "es": "Limitaciones pertinentes para interpretar o comparar",
                "pt": "Limitações relevantes para interpretar ou comparar",
                "ar": "القيود المفيدة للتفسير أو المقارنة"
              },
              "type": "single",
              "options": [
                {
                  "code": 1,
                  "label": {
                    "fr": "Présente dans la réponse",
                    "en": "Included in the response",
                    "es": "Incluida en la respuesta",
                    "pt": "Presente na resposta",
                    "ar": "واردة في الإجابة"
                  }
                },
                {
                  "code": 2,
                  "label": {
                    "fr": "Accessible par un lien ou une référence précise transmis avec la réponse",
                    "en": "Available through a specific link or reference sent with the response",
                    "es": "Accesible mediante un enlace o referencia precisa enviados con la respuesta",
                    "pt": "Acessível por uma ligação ou referência precisa enviada com a resposta",
                    "ar": "متاحة عبر رابط أو مرجع دقيق أرفق بالإجابة"
                  }
                },
                {
                  "code": 0,
                  "label": {
                    "fr": "Absente",
                    "en": "Absent",
                    "es": "Ausente",
                    "pt": "Ausente",
                    "ar": "غير موجودة"
                  }
                },
                {
                  "code": 98,
                  "label": {
                    "fr": "Ne sait pas",
                    "en": "Do not know",
                    "es": "No sabe",
                    "pt": "Não sabe",
                    "ar": "لا أعرف"
                  }
                },
                {
                  "code": 99,
                  "label": {
                    "fr": "Non applicable",
                    "en": "Not applicable",
                    "es": "No aplicable",
                    "pt": "Não aplicável",
                    "ar": "لا ينطبق"
                  }
                }
              ],
              "gate": {
                "all": [
                  {
                    "field": "QP01_Participation",
                    "op": "in",
                    "values": [
                      1
                    ]
                  },
                  {
                    "field": "QP01_Produit",
                    "op": "not_in",
                    "values": [
                      0
                    ]
                  }
                ]
              },
              "source": "Rapport méthodologique, annexe 1bis, pp. 23–27, QP05"
            },
            {
              "id": "QP05_S8",
              "label": {
                "fr": "Statut provisoire ou révisé, lorsqu’il s’applique",
                "en": "Provisional or revised status, where applicable",
                "es": "Carácter provisional o revisado, cuando proceda",
                "pt": "Caráter provisório ou revisto, quando aplicável",
                "ar": "صفة مؤقتة أو منقحة، عند الاقتضاء"
              },
              "type": "single",
              "options": [
                {
                  "code": 1,
                  "label": {
                    "fr": "Présente dans la réponse",
                    "en": "Included in the response",
                    "es": "Incluida en la respuesta",
                    "pt": "Presente na resposta",
                    "ar": "واردة في الإجابة"
                  }
                },
                {
                  "code": 2,
                  "label": {
                    "fr": "Accessible par un lien ou une référence précise transmis avec la réponse",
                    "en": "Available through a specific link or reference sent with the response",
                    "es": "Accesible mediante un enlace o referencia precisa enviados con la respuesta",
                    "pt": "Acessível por uma ligação ou referência precisa enviada com a resposta",
                    "ar": "متاحة عبر رابط أو مرجع دقيق أرفق بالإجابة"
                  }
                },
                {
                  "code": 0,
                  "label": {
                    "fr": "Absente",
                    "en": "Absent",
                    "es": "Ausente",
                    "pt": "Ausente",
                    "ar": "غير موجودة"
                  }
                },
                {
                  "code": 98,
                  "label": {
                    "fr": "Ne sait pas",
                    "en": "Do not know",
                    "es": "No sabe",
                    "pt": "Não sabe",
                    "ar": "لا أعرف"
                  }
                },
                {
                  "code": 99,
                  "label": {
                    "fr": "Non applicable",
                    "en": "Not applicable",
                    "es": "No aplicable",
                    "pt": "Não aplicável",
                    "ar": "لا ينطبق"
                  }
                }
              ],
              "gate": {
                "all": [
                  {
                    "field": "QP01_Participation",
                    "op": "in",
                    "values": [
                      1
                    ]
                  },
                  {
                    "field": "QP01_Produit",
                    "op": "not_in",
                    "values": [
                      0
                    ]
                  }
                ]
              },
              "source": "Rapport méthodologique, annexe 1bis, pp. 23–27, QP05"
            },
            {
              "id": "QP05c",
              "label": {
                "fr": "Pour les données administratives, l’unité comptée était-elle explicitement précisée : personnes distinctes, actes, dossiers, établissements ou autre unité ?",
                "en": "For administrative data, was the counting unit explicitly stated: distinct persons, events, files, establishments or another unit?",
                "es": "Para datos administrativos, ¿se especificó la unidad contada : personas distintas, actos, expedientes, establecimientos u otra unidad?",
                "pt": "Para dados administrativos, foi indicada a unidade contada : pessoas distintas, atos, processos, estabelecimentos ou outra unidade?",
                "ar": "بالنسبة للبيانات الإدارية، هل حددت وحدة العد صراحة : أشخاص متميزون أو معاملات أو ملفات أو منشآت أو وحدة أخرى؟"
              },
              "type": "single",
              "options": [
                {
                  "code": 1,
                  "label": {
                    "fr": "Oui",
                    "en": "Yes",
                    "es": "Sí",
                    "pt": "Sim",
                    "ar": "نعم"
                  }
                },
                {
                  "code": 0,
                  "label": {
                    "fr": "Non",
                    "en": "No",
                    "es": "No",
                    "pt": "Não",
                    "ar": "لا"
                  }
                },
                {
                  "code": 98,
                  "label": {
                    "fr": "Ne sait pas",
                    "en": "Do not know",
                    "es": "No sabe",
                    "pt": "Não sabe",
                    "ar": "لا أعرف"
                  }
                }
              ],
              "gate": {
                "all": [
                  {
                    "field": "QP01_Participation",
                    "op": "in",
                    "values": [
                      1
                    ]
                  },
                  {
                    "all": [
                      {
                        "field": "QP01_Produit",
                        "op": "not_in",
                        "values": [
                          0
                        ]
                      },
                      {
                        "field": "QP05_Origine",
                        "op": "in",
                        "values": [
                          2
                        ]
                      }
                    ]
                  }
                ]
              },
              "source": "Rapport méthodologique, annexe 1bis, pp. 23–27, QP05"
            },
            {
              "id": "QP05c_Precision",
              "label": {
                "fr": "Facultatif : préciser l’unité et une limite connue de couverture ou de doublons, sans donnée individuelle",
                "en": "Optional: specify the unit and a known coverage or duplication limitation, without individual-level data",
                "es": "Opcional : especifique la unidad y una limitación conocida de cobertura o duplicación, sin datos individuales",
                "pt": "Facultativo : especifique a unidade e uma limitação conhecida de cobertura ou duplicação, sem dados individuais",
                "ar": "اختياري : حدد الوحدة وقيدا معروفا للتغطية أو التكرار، دون بيانات فردية"
              },
              "type": "textarea",
              "options": [],
              "max_chars": 2500,
              "gate": {
                "all": [
                  {
                    "field": "QP01_Participation",
                    "op": "in",
                    "values": [
                      1
                    ]
                  },
                  {
                    "all": [
                      {
                        "field": "QP01_Produit",
                        "op": "not_in",
                        "values": [
                          0
                        ]
                      },
                      {
                        "field": "QP05_Origine",
                        "op": "in",
                        "values": [
                          2
                        ]
                      }
                    ]
                  }
                ]
              },
              "source": "Rapport méthodologique, annexe 1bis, pp. 23–27, QP05"
            }
          ],
          "note": {
            "fr": "Considérer le premier chiffre présenté dans la dernière réponse transmise, et non le mieux documenté. Indiquer les informations qui l’accompagnaient effectivement. Une simple mention du nom d’un organisme ne suffit pas pour le code 2. Ne pas recopier une valeur non publique.",
            "en": "Consider the first figure presented in the most recent response, not the best-documented figure. Indicate the information that actually accompanied it. Merely naming an organisation does not qualify for code 2. Do not copy a non-public value.",
            "es": "Considere la primera cifra de la última respuesta enviada, no la mejor documentada. Indique qué información la acompañaba realmente. Mencionar solo el nombre de una entidad no basta para el código 2. No copie un valor no público.",
            "pt": "Considere o primeiro número da última resposta enviada, não o mais bem documentado. Indique as informações que o acompanhavam efetivamente. Mencionar apenas o nome de uma entidade não basta para o código 2. Não copie um valor não público.",
            "ar": "اعتمد أول رقم ورد في آخر إجابة مرسلة، لا الرقم الأفضل توثيقا. حدد المعلومات التي رافقته فعليا. لا يكفي ذكر اسم جهة فقط لاختيار الرمز 2. لا تنقل قيمة غير متاحة للعموم."
          },
          "gate": {
            "all": [
              {
                "field": "QP02a",
                "op": "not_in",
                "values": [
                  0
                ]
              },
              {
                "field": "QP03_Situation",
                "op": "in",
                "values": [
                  1
                ]
              }
            ]
          },
          "theme": "Sources et métadonnées",
          "source": "Rapport méthodologique, annexe 1bis, pp. 23–27, QP05"
        },
        {
          "id": "QP06",
          "title": {
            "fr": "Vérification et validation effectivement réalisées",
            "en": "Checking and validation actually performed",
            "es": "Comprobación y validación efectivamente realizadas",
            "pt": "Verificação e validação efetivamente realizadas",
            "ar": "المراجعة والتحقق المنجزان فعليا"
          },
          "fields": [
            {
              "id": "QP06a",
              "label": {
                "fr": "Qui a vérifié le contenu statistique avant son envoi ? Plusieurs réponses possibles.",
                "en": "Who checked the statistical content before it was sent? Multiple answers allowed.",
                "es": "¿Quién comprobó el contenido estadístico antes de enviarlo? Se admiten varias respuestas.",
                "pt": "Quem verificou o conteúdo estatístico antes do envio? São admitidas várias respostas.",
                "ar": "من راجع المحتوى الإحصائي قبل إرساله؟ يمكن اختيار عدة إجابات."
              },
              "type": "multi",
              "options": [
                {
                  "code": 1,
                  "label": {
                    "fr": "L’agent qui l’a préparé",
                    "en": "The staff member who prepared it",
                    "es": "El agente que lo preparó",
                    "pt": "O agente que o preparou",
                    "ar": "الموظف الذي أعده"
                  }
                },
                {
                  "code": 2,
                  "label": {
                    "fr": "Un autre agent producteur des données",
                    "en": "Another staff member producing the data",
                    "es": "Otro agente productor de los datos",
                    "pt": "Outro agente produtor dos dados",
                    "ar": "موظف آخر منتج للبيانات"
                  }
                },
                {
                  "code": 3,
                  "label": {
                    "fr": "Un responsable technique ou de qualité",
                    "en": "A technical or quality manager",
                    "es": "Un responsable técnico o de calidad",
                    "pt": "Um responsável técnico ou de qualidade",
                    "ar": "مسؤول فني أو مسؤول جودة"
                  }
                },
                {
                  "code": 4,
                  "label": {
                    "fr": "Le service de communication",
                    "en": "The communication service",
                    "es": "El servicio de comunicación",
                    "pt": "O serviço de comunicação",
                    "ar": "إدارة الاتصال"
                  }
                },
                {
                  "code": 5,
                  "label": {
                    "fr": "La direction ou son délégataire",
                    "en": "Management or its delegate",
                    "es": "La dirección o su delegado",
                    "pt": "A direção ou o seu delegado",
                    "ar": "الإدارة العليا أو من تفوضه"
                  }
                },
                {
                  "code": 0,
                  "label": {
                    "fr": "Aucun contrôle connu",
                    "en": "No known check",
                    "es": "Ningún control conocido",
                    "pt": "Nenhum controlo conhecido",
                    "ar": "لا أعرف أي مراجعة"
                  }
                },
                {
                  "code": 97,
                  "label": {
                    "fr": "Autre fonction",
                    "en": "Other function",
                    "es": "Otra función",
                    "pt": "Outra função",
                    "ar": "وظيفة أخرى"
                  }
                },
                {
                  "code": 98,
                  "label": {
                    "fr": "Ne sait pas",
                    "en": "Do not know",
                    "es": "No sabe",
                    "pt": "Não sabe",
                    "ar": "لا أعرف"
                  }
                },
                {
                  "code": 6,
                  "label": {
                    "fr": "Agent de l’INS",
                    "en": "NSO staff member",
                    "es": "Agente de la ONE",
                    "pt": "Agente do INE",
                    "ar": "موظف الجهاز الوطني للإحصاء"
                  }
                },
                {
                  "code": 7,
                  "label": {
                    "fr": "Autre producteur externe",
                    "en": "Another external producer",
                    "es": "Otro productor externo",
                    "pt": "Outro produtor externo",
                    "ar": "منتج خارجي آخر"
                  }
                }
              ],
              "exclusive": [
                0,
                98
              ],
              "gate": {
                "all": [
                  {
                    "field": "QP01_Participation",
                    "op": "in",
                    "values": [
                      1
                    ]
                  },
                  {
                    "field": "QP01_Produit",
                    "op": "not_in",
                    "values": [
                      0
                    ]
                  }
                ]
              },
              "source": "Rapport méthodologique, annexe 1bis, pp. 23–27, QP06"
            },
            {
              "id": "QP06a_Autre",
              "label": {
                "fr": "Autre : précisez",
                "en": "Other: please specify",
                "es": "Otra : especifique",
                "pt": "Outra : especifique",
                "ar": "أخرى : يرجى التحديد"
              },
              "type": "text",
              "options": [],
              "max_chars": 500,
              "gate": {
                "all": [
                  {
                    "field": "QP01_Participation",
                    "op": "in",
                    "values": [
                      1
                    ]
                  },
                  {
                    "all": [
                      {
                        "field": "QP01_Produit",
                        "op": "not_in",
                        "values": [
                          0
                        ]
                      },
                      {
                        "field": "QP06a",
                        "op": "contains",
                        "values": [
                          97
                        ]
                      }
                    ]
                  }
                ]
              },
              "source": "Rapport méthodologique, annexe 1bis, pp. 23–27, QP06"
            },
            {
              "id": "QP06b",
              "label": {
                "fr": "Quelle trace de la validation statistique finale existe pour cet envoi ?",
                "en": "What record exists of the final statistical validation for this submission?",
                "es": "¿Qué constancia existe de la validación estadística final de este envío?",
                "pt": "Que registo existe da validação estatística final deste envio?",
                "ar": "ما الإثبات الموجود للتحقق الإحصائي النهائي لهذا الإرسال؟"
              },
              "type": "single",
              "options": [
                {
                  "code": 1,
                  "label": {
                    "fr": "Trace écrite, courriel, visa ou journal de validation",
                    "en": "Written record, email, sign-off or validation log",
                    "es": "Constancia escrita, correo, visado o registro de validación",
                    "pt": "Registo escrito, correio, visto ou diário de validação",
                    "ar": "إثبات كتابي أو بريد أو تأشيرة اعتماد أو سجل تحقق"
                  }
                },
                {
                  "code": 2,
                  "label": {
                    "fr": "Accord oral seulement",
                    "en": "Oral agreement only",
                    "es": "Solo acuerdo oral",
                    "pt": "Apenas acordo oral",
                    "ar": "موافقة شفهية فقط"
                  }
                },
                {
                  "code": 0,
                  "label": {
                    "fr": "Aucune validation distincte connue",
                    "en": "No separate validation known",
                    "es": "Ninguna validación independiente conocida",
                    "pt": "Nenhuma validação distinta conhecida",
                    "ar": "لا أعرف تحققا منفصلا"
                  }
                },
                {
                  "code": 98,
                  "label": {
                    "fr": "Ne sait pas",
                    "en": "Do not know",
                    "es": "No sabe",
                    "pt": "Não sabe",
                    "ar": "لا أعرف"
                  }
                }
              ],
              "gate": {
                "all": [
                  {
                    "field": "QP01_Participation",
                    "op": "in",
                    "values": [
                      1
                    ]
                  },
                  {
                    "field": "QP01_Produit",
                    "op": "not_in",
                    "values": [
                      0
                    ]
                  }
                ]
              },
              "source": "Rapport méthodologique, annexe 1bis, pp. 23–27, QP06"
            }
          ],
          "note": {
            "fr": "Il s’agit du contenu statistique, pas de l’approbation politique du discours. L’absence d’intervention de l’INS ne signifie pas, à elle seule, absence de contrôle.",
            "en": "This concerns statistical content, not political approval of the speech. The absence of NSO involvement does not by itself mean that there was no check.",
            "es": "Se trata del contenido estadístico, no de la aprobación política del discurso. La ausencia de intervención de la ONE no implica por sí sola ausencia de control.",
            "pt": "Trata-se do conteúdo estatístico, não da aprovação política do discurso. A ausência de intervenção do INE não implica, por si só, ausência de controlo.",
            "ar": "المقصود المحتوى الإحصائي، وليس الموافقة السياسية على الخطاب. عدم تدخل الجهاز الوطني للإحصاء لا يعني وحده غياب المراجعة."
          },
          "gate": {
            "all": [
              {
                "field": "QP02a",
                "op": "not_in",
                "values": [
                  0
                ]
              },
              {
                "field": "QP03_Situation",
                "op": "in",
                "values": [
                  1
                ]
              }
            ]
          },
          "theme": "Vérification",
          "source": "Rapport méthodologique, annexe 1bis, pp. 23–27, QP06"
        },
        {
          "id": "QP07",
          "title": {
            "fr": "Format transmis et vérification du passage de discours",
            "en": "Format provided and checking of the speech passage",
            "es": "Formato facilitado y comprobación del pasaje del discurso",
            "pt": "Formato fornecido e verificação do trecho do discurso",
            "ar": "صيغة الدعم المقدم ومراجعة مقطع الخطاب"
          },
          "fields": [
            {
              "id": "QP07a",
              "label": {
                "fr": "Sous quelle forme l’appui a-t-il été transmis ? Jusqu’à trois réponses.",
                "en": "In what form was the support provided? Up to three answers.",
                "es": "¿En qué formato se proporcionó el apoyo? Hasta tres respuestas.",
                "pt": "Em que formato foi fornecido o apoio? Até três respostas.",
                "ar": "بأي صيغة قدم الدعم؟ ثلاث إجابات كحد أقصى."
              },
              "type": "multi",
              "options": [
                {
                  "code": 1,
                  "label": {
                    "fr": "Tableau ou liste de chiffres",
                    "en": "Table or list of figures",
                    "es": "Tabla o lista de cifras",
                    "pt": "Tabela ou lista de números",
                    "ar": "جدول أو قائمة أرقام"
                  }
                },
                {
                  "code": 2,
                  "label": {
                    "fr": "Fiche accompagnée d’explications",
                    "en": "Fact sheet with explanations",
                    "es": "Ficha con explicaciones",
                    "pt": "Ficha com explicações",
                    "ar": "بطاقة مع توضيحات"
                  }
                },
                {
                  "code": 3,
                  "label": {
                    "fr": "Proposition de phrases ou de paragraphes",
                    "en": "Suggested sentences or paragraphs",
                    "es": "Propuesta de frases o párrafos",
                    "pt": "Proposta de frases ou parágrafos",
                    "ar": "اقتراح جمل أو فقرات"
                  }
                },
                {
                  "code": 4,
                  "label": {
                    "fr": "Graphique ou illustration",
                    "en": "Chart or illustration",
                    "es": "Gráfico o ilustración",
                    "pt": "Gráfico ou ilustração",
                    "ar": "رسم بياني أو توضيحي"
                  }
                },
                {
                  "code": 5,
                  "label": {
                    "fr": "Explication orale uniquement",
                    "en": "Oral explanation only",
                    "es": "Solo explicación oral",
                    "pt": "Apenas explicação oral",
                    "ar": "شرح شفهي فقط"
                  }
                },
                {
                  "code": 97,
                  "label": {
                    "fr": "Autre",
                    "en": "Other",
                    "es": "Otra",
                    "pt": "Outra",
                    "ar": "أخرى"
                  }
                },
                {
                  "code": 98,
                  "label": {
                    "fr": "Ne sait pas",
                    "en": "Do not know",
                    "es": "No sabe",
                    "pt": "Não sabe",
                    "ar": "لا أعرف"
                  }
                }
              ],
              "max_choices": 3,
              "exclusive": [
                98
              ],
              "gate": {
                "all": [
                  {
                    "field": "QP01_Participation",
                    "op": "in",
                    "values": [
                      1
                    ]
                  },
                  {
                    "field": "QP01_Produit",
                    "op": "not_in",
                    "values": [
                      0
                    ]
                  }
                ]
              },
              "source": "Rapport méthodologique, annexe 1bis, pp. 23–27, QP07"
            },
            {
              "id": "QP07a_Autre",
              "label": {
                "fr": "Autre : précisez",
                "en": "Other: please specify",
                "es": "Otra : especifique",
                "pt": "Outra : especifique",
                "ar": "أخرى : يرجى التحديد"
              },
              "type": "text",
              "options": [],
              "max_chars": 500,
              "gate": {
                "all": [
                  {
                    "field": "QP01_Participation",
                    "op": "in",
                    "values": [
                      1
                    ]
                  },
                  {
                    "all": [
                      {
                        "field": "QP01_Produit",
                        "op": "not_in",
                        "values": [
                          0
                        ]
                      },
                      {
                        "field": "QP07a",
                        "op": "contains",
                        "values": [
                          97
                        ]
                      }
                    ]
                  }
                ]
              },
              "source": "Rapport méthodologique, annexe 1bis, pp. 23–27, QP07"
            },
            {
              "id": "QP07b",
              "label": {
                "fr": "Votre organisme a-t-il pu vérifier le passage de discours avant diffusion ?",
                "en": "Was your organisation able to check the speech passage before dissemination?",
                "es": "¿Pudo su organismo comprobar el pasaje del discurso antes de su difusión?",
                "pt": "O seu organismo pôde verificar o trecho do discurso antes da divulgação?",
                "ar": "هل تمكنت مؤسستكم من مراجعة مقطع الخطاب قبل نشره؟"
              },
              "type": "single",
              "options": [
                {
                  "code": 1,
                  "label": {
                    "fr": "Oui",
                    "en": "Yes",
                    "es": "Sí",
                    "pt": "Sim",
                    "ar": "نعم"
                  }
                },
                {
                  "code": 2,
                  "label": {
                    "fr": "Non, le passage n’a pas été reçu",
                    "en": "No, the passage was not received",
                    "es": "No, no se recibió el pasaje",
                    "pt": "Não, o trecho não foi recebido",
                    "ar": "لا، لم يستلم المقطع"
                  }
                },
                {
                  "code": 3,
                  "label": {
                    "fr": "Non, le passage a été reçu mais n’a pas été vérifié",
                    "en": "No, the passage was received but not checked",
                    "es": "No, se recibió el pasaje pero no se comprobó",
                    "pt": "Não, o trecho foi recebido mas não foi verificado",
                    "ar": "لا، استلم المقطع لكنه لم يراجع"
                  }
                },
                {
                  "code": 4,
                  "label": {
                    "fr": "Le discours n’a pas été diffusé ou cette étape n’a pas eu lieu",
                    "en": "The speech was not delivered or this stage did not occur",
                    "es": "El discurso no se difundió o esta etapa no tuvo lugar",
                    "pt": "O discurso não foi divulgado ou esta etapa não ocorreu",
                    "ar": "لم ينشر الخطاب أو لم تتم هذه المرحلة"
                  }
                },
                {
                  "code": 98,
                  "label": {
                    "fr": "Ne sait pas",
                    "en": "Do not know",
                    "es": "No sabe",
                    "pt": "Não sabe",
                    "ar": "لا أعرف"
                  }
                }
              ],
              "gate": {
                "all": [
                  {
                    "field": "QP01_Participation",
                    "op": "in",
                    "values": [
                      1
                    ]
                  },
                  {
                    "field": "QP01_Produit",
                    "op": "not_in",
                    "values": [
                      0
                    ]
                  }
                ]
              },
              "source": "Rapport méthodologique, annexe 1bis, pp. 23–27, QP07"
            },
            {
              "id": "QP07c",
              "label": {
                "fr": "Un retour sur l’utilisation des chiffres a-t-il été reçu ?",
                "en": "Was feedback received on the use of the figures?",
                "es": "¿Se recibieron comentarios sobre el uso de las cifras?",
                "pt": "Foi recebido retorno sobre a utilização dos números?",
                "ar": "هل وردت ملاحظات حول استخدام الأرقام؟"
              },
              "type": "single",
              "options": [
                {
                  "code": 1,
                  "label": {
                    "fr": "Texte ou lien du discours",
                    "en": "Text or link to the speech",
                    "es": "Texto o enlace al discurso",
                    "pt": "Texto ou ligação para o discurso",
                    "ar": "نص الخطاب أو رابطه"
                  }
                },
                {
                  "code": 2,
                  "label": {
                    "fr": "Commentaire sans texte ni lien",
                    "en": "Comment without text or link",
                    "es": "Comentario sin texto ni enlace",
                    "pt": "Comentário sem texto nem ligação",
                    "ar": "تعليق دون نص أو رابط"
                  }
                },
                {
                  "code": 3,
                  "label": {
                    "fr": "Les deux",
                    "en": "Both",
                    "es": "Ambos",
                    "pt": "Ambos",
                    "ar": "كلاهما"
                  }
                },
                {
                  "code": 0,
                  "label": {
                    "fr": "Aucun",
                    "en": "None",
                    "es": "Ninguno",
                    "pt": "Nenhum",
                    "ar": "لا يوجد"
                  }
                },
                {
                  "code": 98,
                  "label": {
                    "fr": "Ne sait pas",
                    "en": "Do not know",
                    "es": "No sabe",
                    "pt": "Não sabe",
                    "ar": "لا أعرف"
                  }
                },
                {
                  "code": 99,
                  "label": {
                    "fr": "Non applicable",
                    "en": "Not applicable",
                    "es": "No aplicable",
                    "pt": "Não aplicável",
                    "ar": "لا ينطبق"
                  }
                }
              ],
              "gate": {
                "all": [
                  {
                    "field": "QP01_Participation",
                    "op": "in",
                    "values": [
                      1
                    ]
                  },
                  {
                    "field": "QP01_Produit",
                    "op": "not_in",
                    "values": [
                      0
                    ]
                  }
                ]
              },
              "source": "Rapport méthodologique, annexe 1bis, pp. 23–27, QP07"
            }
          ],
          "note": {
            "fr": "Le retour documente l’usage des chiffres, pas l’effet de la décision.",
            "en": "Feedback documents the use of figures, not the effect of the decision.",
            "es": "Los comentarios documentan el uso de las cifras, no el efecto de la decisión.",
            "pt": "O retorno documenta a utilização dos números, não o efeito da decisão.",
            "ar": "توثق الملاحظات استخدام الأرقام، لا أثر القرار."
          },
          "gate": {
            "all": [
              {
                "field": "QP02a",
                "op": "not_in",
                "values": [
                  0
                ]
              },
              {
                "field": "QP03_Situation",
                "op": "in",
                "values": [
                  1
                ]
              }
            ]
          },
          "theme": "Messages et publics",
          "source": "Rapport méthodologique, annexe 1bis, pp. 23–27, QP07"
        },
        {
          "id": "QP08",
          "title": {
            "fr": "Correction ou actualisation après transmission",
            "en": "Correction or updating after transmission",
            "es": "Corrección o actualización después del envío",
            "pt": "Correção ou atualização após o envio",
            "ar": "التصحيح أو التحديث بعد الإرسال"
          },
          "fields": [
            {
              "id": "QP08_Situation",
              "label": {
                "fr": "Pendant la période de référence, avez-vous connaissance d’un appui déjà transmis qu’il a fallu corriger ou actualiser ?",
                "en": "During the reference period, do you know of support already provided that had to be corrected or updated?",
                "es": "Durante el período de referencia, ¿conoce algún apoyo ya enviado que debiera corregirse o actualizarse?",
                "pt": "Durante o período de referência, conhece algum apoio já enviado que tenha exigido correção ou atualização?",
                "ar": "هل تعرف دعما سبق إرساله واستلزم التصحيح أو التحديث خلال الفترة المرجعية؟"
              },
              "type": "single",
              "options": [
                {
                  "code": 1,
                  "label": {
                    "fr": "Oui, pour corriger une erreur",
                    "en": "Yes, to correct an error",
                    "es": "Sí, para corregir un error",
                    "pt": "Sim, para corrigir um erro",
                    "ar": "نعم، لتصحيح خطأ"
                  }
                },
                {
                  "code": 2,
                  "label": {
                    "fr": "Oui, pour actualiser une donnée",
                    "en": "Yes, to update data",
                    "es": "Sí, para actualizar un dato",
                    "pt": "Sim, para atualizar um dado",
                    "ar": "نعم، لتحديث بيانات"
                  }
                },
                {
                  "code": 3,
                  "label": {
                    "fr": "Les deux situations ont été rencontrées",
                    "en": "Both situations occurred",
                    "es": "Se produjeron ambas situaciones",
                    "pt": "Ocorreram ambas as situações",
                    "ar": "حدثت الحالتان"
                  }
                },
                {
                  "code": 0,
                  "label": {
                    "fr": "Aucun épisode connu",
                    "en": "No known instance",
                    "es": "Ningún episodio conocido",
                    "pt": "Nenhum episódio conhecido",
                    "ar": "لا أعرف أي حالة"
                  }
                },
                {
                  "code": 98,
                  "label": {
                    "fr": "Ne sait pas",
                    "en": "Do not know",
                    "es": "No sabe",
                    "pt": "Não sabe",
                    "ar": "لا أعرف"
                  }
                },
                {
                  "code": 99,
                  "label": {
                    "fr": "Non applicable, aucun appui concerné",
                    "en": "Not applicable, no relevant support",
                    "es": "No aplicable, ningún apoyo pertinente",
                    "pt": "Não aplicável, nenhum apoio em causa",
                    "ar": "لا ينطبق، لا يوجد دعم معني"
                  }
                }
              ],
              "gate": {
                "all": [
                  {
                    "field": "QP01_Participation",
                    "op": "in",
                    "values": [
                      1
                    ]
                  },
                  {
                    "field": "QP01_Produit",
                    "op": "not_in",
                    "values": [
                      0
                    ]
                  }
                ]
              },
              "source": "Rapport méthodologique, annexe 1bis, pp. 23–27, QP08"
            },
            {
              "id": "QP08_Actions",
              "label": {
                "fr": "Pour le cas le plus récent, quelles actions ont été réalisées ? Plusieurs réponses possibles.",
                "en": "For the most recent case, what actions were taken? Multiple answers allowed.",
                "es": "En el caso más reciente, ¿qué acciones se realizaron? Se admiten varias respuestas.",
                "pt": "No caso mais recente, que ações foram realizadas? São admitidas várias respostas.",
                "ar": "ما الإجراءات المتخذة في أحدث حالة؟ يمكن اختيار عدة إجابات."
              },
              "type": "multi",
              "options": [
                {
                  "code": 1,
                  "label": {
                    "fr": "Alerte de l’équipe préparant le discours",
                    "en": "Alerting the speech-preparation team",
                    "es": "Aviso al equipo que prepara el discurso",
                    "pt": "Alerta à equipa que prepara o discurso",
                    "ar": "تنبيه فريق إعداد الخطاب"
                  }
                },
                {
                  "code": 2,
                  "label": {
                    "fr": "Envoi d’une version corrigée avec sa source",
                    "en": "Sending a corrected version with its source",
                    "es": "Envío de una versión corregida con su fuente",
                    "pt": "Envio de uma versão corrigida com a fonte",
                    "ar": "إرسال نسخة مصححة مع مصدرها"
                  }
                },
                {
                  "code": 3,
                  "label": {
                    "fr": "Conservation des versions et du motif de changement",
                    "en": "Keeping versions and the reason for the change",
                    "es": "Conservación de las versiones y del motivo del cambio",
                    "pt": "Conservação das versões e do motivo da alteração",
                    "ar": "حفظ النسخ وسبب التغيير"
                  }
                },
                {
                  "code": 4,
                  "label": {
                    "fr": "Demande de correction d’un passage déjà diffusé",
                    "en": "Requesting correction of a passage already disseminated",
                    "es": "Solicitud de corrección de un pasaje ya difundido",
                    "pt": "Pedido de correção de um trecho já divulgado",
                    "ar": "طلب تصحيح مقطع سبق نشره"
                  }
                },
                {
                  "code": 5,
                  "label": {
                    "fr": "Vérification de la prise en compte de la correction",
                    "en": "Checking that the correction was incorporated",
                    "es": "Verificación de que se incorporó la corrección",
                    "pt": "Verificação de que a correção foi incorporada",
                    "ar": "التحقق من أخذ التصحيح في الاعتبار"
                  }
                },
                {
                  "code": 0,
                  "label": {
                    "fr": "Aucune action connue",
                    "en": "No known action",
                    "es": "Ninguna acción conocida",
                    "pt": "Nenhuma ação conhecida",
                    "ar": "لا أعرف أي إجراء"
                  }
                },
                {
                  "code": 97,
                  "label": {
                    "fr": "Autre",
                    "en": "Other",
                    "es": "Otra",
                    "pt": "Outra",
                    "ar": "أخرى"
                  }
                },
                {
                  "code": 98,
                  "label": {
                    "fr": "Ne sait pas",
                    "en": "Do not know",
                    "es": "No sabe",
                    "pt": "Não sabe",
                    "ar": "لا أعرف"
                  }
                },
                {
                  "code": 6,
                  "label": {
                    "fr": "Information de l’INS ou de l’intermédiaire concerné",
                    "en": "Informing the NSO or relevant intermediary",
                    "es": "Información a la ONE o al intermediario correspondiente",
                    "pt": "Informação ao INE ou ao intermediário em causa",
                    "ar": "إبلاغ الجهاز الوطني للإحصاء أو الوسيط المعني"
                  }
                }
              ],
              "exclusive": [
                0,
                98
              ],
              "gate": {
                "all": [
                  {
                    "field": "QP01_Participation",
                    "op": "in",
                    "values": [
                      1
                    ]
                  },
                  {
                    "all": [
                      {
                        "field": "QP01_Produit",
                        "op": "not_in",
                        "values": [
                          0
                        ]
                      },
                      {
                        "field": "QP08_Situation",
                        "op": "in",
                        "values": [
                          1,
                          2,
                          3
                        ]
                      }
                    ]
                  }
                ]
              },
              "source": "Rapport méthodologique, annexe 1bis, pp. 23–27, QP08"
            },
            {
              "id": "QP08_Actions_Autre",
              "label": {
                "fr": "Autre : précisez",
                "en": "Other: please specify",
                "es": "Otra : especifique",
                "pt": "Outra : especifique",
                "ar": "أخرى : يرجى التحديد"
              },
              "type": "text",
              "options": [],
              "max_chars": 500,
              "gate": {
                "all": [
                  {
                    "field": "QP01_Participation",
                    "op": "in",
                    "values": [
                      1
                    ]
                  },
                  {
                    "all": [
                      {
                        "field": "QP01_Produit",
                        "op": "not_in",
                        "values": [
                          0
                        ]
                      },
                      {
                        "field": "QP08_Actions",
                        "op": "contains",
                        "values": [
                          97
                        ]
                      }
                    ]
                  }
                ]
              },
              "source": "Rapport méthodologique, annexe 1bis, pp. 23–27, QP08"
            }
          ],
          "note": {
            "fr": "Un appui initialement fourni avant la période peut être pris en compte si la correction intervient pendant celle-ci. Cette question reste accessible même si aucune nouvelle demande n’a été reçue.",
            "en": "Support originally provided before the period may be included if the correction occurred during the period. This question remains available even if no new request was received.",
            "es": "Puede incluirse un apoyo enviado antes del período si la corrección se produjo durante este. Esta pregunta sigue disponible aunque no se recibieran solicitudes nuevas.",
            "pt": "Pode incluir-se um apoio enviado antes do período se a correção ocorreu durante o mesmo. Esta pergunta mantém-se disponível mesmo sem novos pedidos.",
            "ar": "يمكن إدراج دعم قدم قبل الفترة إذا جرى تصحيحه خلالها. يبقى هذا السؤال متاحا حتى عند عدم استلام طلب جديد."
          },
          "gate": null,
          "theme": "Actualisation et correction",
          "source": "Rapport méthodologique, annexe 1bis, pp. 23–27, QP08"
        },
        {
          "id": "QP09",
          "title": {
            "fr": "Organisation et coordination avec l’INS",
            "en": "Organisation and coordination with the NSO",
            "es": "Organización y coordinación con la ONE",
            "pt": "Organização e coordenação com o INE",
            "ar": "التنظيم والتنسيق مع الجهاز الوطني للإحصاء"
          },
          "fields": [
            {
              "id": "QP09_D1",
              "label": {
                "fr": "Point d’entrée ou fonction identifiée pour recevoir les demandes",
                "en": "Identified contact point or function for receiving requests",
                "es": "Punto de contacto o función identificada para recibir solicitudes",
                "pt": "Ponto de contacto ou função identificada para receber pedidos",
                "ar": "نقطة اتصال أو وظيفة محددة لاستلام الطلبات"
              },
              "type": "single",
              "options": [
                {
                  "code": 1,
                  "label": {
                    "fr": "Existe et a été utilisé pour ces appuis pendant la période",
                    "en": "Exists and was used for this support during the period",
                    "es": "Existe y se utilizó para estos apoyos durante el período",
                    "pt": "Existe e foi utilizado para estes apoios durante o período",
                    "ar": "موجود واستخدم لهذا الدعم خلال الفترة"
                  }
                },
                {
                  "code": 2,
                  "label": {
                    "fr": "Existe mais n’a pas été utilisé pendant la période",
                    "en": "Exists but was not used during the period",
                    "es": "Existe pero no se utilizó durante el período",
                    "pt": "Existe mas não foi utilizado durante o período",
                    "ar": "موجود لكنه لم يستخدم خلال الفترة"
                  }
                },
                {
                  "code": 0,
                  "label": {
                    "fr": "N’existe pas",
                    "en": "Does not exist",
                    "es": "No existe",
                    "pt": "Não existe",
                    "ar": "غير موجود"
                  }
                },
                {
                  "code": 98,
                  "label": {
                    "fr": "Ne sait pas",
                    "en": "Do not know",
                    "es": "No sabe",
                    "pt": "Não sabe",
                    "ar": "لا أعرف"
                  }
                },
                {
                  "code": 99,
                  "label": {
                    "fr": "Non applicable",
                    "en": "Not applicable",
                    "es": "No aplicable",
                    "pt": "Não aplicável",
                    "ar": "لا ينطبق"
                  }
                }
              ],
              "gate": {
                "all": [
                  {
                    "field": "QP01_Participation",
                    "op": "in",
                    "values": [
                      1
                    ]
                  },
                  {
                    "field": "QP01_Produit",
                    "op": "not_in",
                    "values": [
                      0
                    ]
                  }
                ]
              },
              "source": "Rapport méthodologique, annexe 1bis, pp. 23–27, QP09"
            },
            {
              "id": "QP09_D2",
              "label": {
                "fr": "Consigne écrite de traitement, y compris des urgences",
                "en": "Written handling instructions, including for urgent requests",
                "es": "Instrucciones escritas de tratamiento, incluidas las urgencias",
                "pt": "Instruções escritas de tratamento, incluindo urgências",
                "ar": "تعليمات معالجة مكتوبة، بما فيها الحالات العاجلة"
              },
              "type": "single",
              "options": [
                {
                  "code": 1,
                  "label": {
                    "fr": "Existe et a été utilisé pour ces appuis pendant la période",
                    "en": "Exists and was used for this support during the period",
                    "es": "Existe y se utilizó para estos apoyos durante el período",
                    "pt": "Existe e foi utilizado para estes apoios durante o período",
                    "ar": "موجود واستخدم لهذا الدعم خلال الفترة"
                  }
                },
                {
                  "code": 2,
                  "label": {
                    "fr": "Existe mais n’a pas été utilisé pendant la période",
                    "en": "Exists but was not used during the period",
                    "es": "Existe pero no se utilizó durante el período",
                    "pt": "Existe mas não foi utilizado durante o período",
                    "ar": "موجود لكنه لم يستخدم خلال الفترة"
                  }
                },
                {
                  "code": 0,
                  "label": {
                    "fr": "N’existe pas",
                    "en": "Does not exist",
                    "es": "No existe",
                    "pt": "Não existe",
                    "ar": "غير موجود"
                  }
                },
                {
                  "code": 98,
                  "label": {
                    "fr": "Ne sait pas",
                    "en": "Do not know",
                    "es": "No sabe",
                    "pt": "Não sabe",
                    "ar": "لا أعرف"
                  }
                },
                {
                  "code": 99,
                  "label": {
                    "fr": "Non applicable",
                    "en": "Not applicable",
                    "es": "No aplicable",
                    "pt": "Não aplicável",
                    "ar": "لا ينطبق"
                  }
                }
              ],
              "gate": {
                "all": [
                  {
                    "field": "QP01_Participation",
                    "op": "in",
                    "values": [
                      1
                    ]
                  },
                  {
                    "field": "QP01_Produit",
                    "op": "not_in",
                    "values": [
                      0
                    ]
                  }
                ]
              },
              "source": "Rapport méthodologique, annexe 1bis, pp. 23–27, QP09"
            },
            {
              "id": "QP09_D3",
              "label": {
                "fr": "Registre des demandes avec échéance, responsable et état de traitement",
                "en": "Request register with deadline, responsible officer and processing status",
                "es": "Registro de solicitudes con plazo, responsable y estado de tramitación",
                "pt": "Registo de pedidos com prazo, responsável e estado de tratamento",
                "ar": "سجل للطلبات يتضمن المهلة والمسؤول وحالة المعالجة"
              },
              "type": "single",
              "options": [
                {
                  "code": 1,
                  "label": {
                    "fr": "Existe et a été utilisé pour ces appuis pendant la période",
                    "en": "Exists and was used for this support during the period",
                    "es": "Existe y se utilizó para estos apoyos durante el período",
                    "pt": "Existe e foi utilizado para estes apoios durante o período",
                    "ar": "موجود واستخدم لهذا الدعم خلال الفترة"
                  }
                },
                {
                  "code": 2,
                  "label": {
                    "fr": "Existe mais n’a pas été utilisé pendant la période",
                    "en": "Exists but was not used during the period",
                    "es": "Existe pero no se utilizó durante el período",
                    "pt": "Existe mas não foi utilizado durante o período",
                    "ar": "موجود لكنه لم يستخدم خلال الفترة"
                  }
                },
                {
                  "code": 0,
                  "label": {
                    "fr": "N’existe pas",
                    "en": "Does not exist",
                    "es": "No existe",
                    "pt": "Não existe",
                    "ar": "غير موجود"
                  }
                },
                {
                  "code": 98,
                  "label": {
                    "fr": "Ne sait pas",
                    "en": "Do not know",
                    "es": "No sabe",
                    "pt": "Não sabe",
                    "ar": "لا أعرف"
                  }
                },
                {
                  "code": 99,
                  "label": {
                    "fr": "Non applicable",
                    "en": "Not applicable",
                    "es": "No aplicable",
                    "pt": "Não aplicável",
                    "ar": "لا ينطبق"
                  }
                }
              ],
              "gate": {
                "all": [
                  {
                    "field": "QP01_Participation",
                    "op": "in",
                    "values": [
                      1
                    ]
                  },
                  {
                    "field": "QP01_Produit",
                    "op": "not_in",
                    "values": [
                      0
                    ]
                  }
                ]
              },
              "source": "Rapport méthodologique, annexe 1bis, pp. 23–27, QP09"
            },
            {
              "id": "QP09_D4",
              "label": {
                "fr": "Répertoire de chiffres avec sources et informations d’interprétation",
                "en": "Repository of figures with sources and interpretation information",
                "es": "Repositorio de cifras con fuentes e información de interpretación",
                "pt": "Repositório de números com fontes e informações de interpretação",
                "ar": "دليل أرقام مع مصادر ومعلومات تفسيرية"
              },
              "type": "single",
              "options": [
                {
                  "code": 1,
                  "label": {
                    "fr": "Existe et a été utilisé pour ces appuis pendant la période",
                    "en": "Exists and was used for this support during the period",
                    "es": "Existe y se utilizó para estos apoyos durante el período",
                    "pt": "Existe e foi utilizado para estes apoios durante o período",
                    "ar": "موجود واستخدم لهذا الدعم خلال الفترة"
                  }
                },
                {
                  "code": 2,
                  "label": {
                    "fr": "Existe mais n’a pas été utilisé pendant la période",
                    "en": "Exists but was not used during the period",
                    "es": "Existe pero no se utilizó durante el período",
                    "pt": "Existe mas não foi utilizado durante o período",
                    "ar": "موجود لكنه لم يستخدم خلال الفترة"
                  }
                },
                {
                  "code": 0,
                  "label": {
                    "fr": "N’existe pas",
                    "en": "Does not exist",
                    "es": "No existe",
                    "pt": "Não existe",
                    "ar": "غير موجود"
                  }
                },
                {
                  "code": 98,
                  "label": {
                    "fr": "Ne sait pas",
                    "en": "Do not know",
                    "es": "No sabe",
                    "pt": "Não sabe",
                    "ar": "لا أعرف"
                  }
                },
                {
                  "code": 99,
                  "label": {
                    "fr": "Non applicable",
                    "en": "Not applicable",
                    "es": "No aplicable",
                    "pt": "Não aplicável",
                    "ar": "لا ينطبق"
                  }
                }
              ],
              "gate": {
                "all": [
                  {
                    "field": "QP01_Participation",
                    "op": "in",
                    "values": [
                      1
                    ]
                  },
                  {
                    "field": "QP01_Produit",
                    "op": "not_in",
                    "values": [
                      0
                    ]
                  }
                ]
              },
              "source": "Rapport méthodologique, annexe 1bis, pp. 23–27, QP09"
            },
            {
              "id": "QP09_D5",
              "label": {
                "fr": "Trace du contrôle et de la validation statistique",
                "en": "Record of statistical checking and validation",
                "es": "Constancia del control y de la validación estadística",
                "pt": "Registo do controlo e da validação estatística",
                "ar": "إثبات المراجعة والتحقق الإحصائي"
              },
              "type": "single",
              "options": [
                {
                  "code": 1,
                  "label": {
                    "fr": "Existe et a été utilisé pour ces appuis pendant la période",
                    "en": "Exists and was used for this support during the period",
                    "es": "Existe y se utilizó para estos apoyos durante el período",
                    "pt": "Existe e foi utilizado para estes apoios durante o período",
                    "ar": "موجود واستخدم لهذا الدعم خلال الفترة"
                  }
                },
                {
                  "code": 2,
                  "label": {
                    "fr": "Existe mais n’a pas été utilisé pendant la période",
                    "en": "Exists but was not used during the period",
                    "es": "Existe pero no se utilizó durante el período",
                    "pt": "Existe mas não foi utilizado durante o período",
                    "ar": "موجود لكنه لم يستخدم خلال الفترة"
                  }
                },
                {
                  "code": 0,
                  "label": {
                    "fr": "N’existe pas",
                    "en": "Does not exist",
                    "es": "No existe",
                    "pt": "Não existe",
                    "ar": "غير موجود"
                  }
                },
                {
                  "code": 98,
                  "label": {
                    "fr": "Ne sait pas",
                    "en": "Do not know",
                    "es": "No sabe",
                    "pt": "Não sabe",
                    "ar": "لا أعرف"
                  }
                },
                {
                  "code": 99,
                  "label": {
                    "fr": "Non applicable",
                    "en": "Not applicable",
                    "es": "No aplicable",
                    "pt": "Não aplicável",
                    "ar": "لا ينطبق"
                  }
                }
              ],
              "gate": {
                "all": [
                  {
                    "field": "QP01_Participation",
                    "op": "in",
                    "values": [
                      1
                    ]
                  },
                  {
                    "field": "QP01_Produit",
                    "op": "not_in",
                    "values": [
                      0
                    ]
                  }
                ]
              },
              "source": "Rapport méthodologique, annexe 1bis, pp. 23–27, QP09"
            },
            {
              "id": "QP09_D6",
              "label": {
                "fr": "Conservation des versions et des corrections",
                "en": "Retention of versions and corrections",
                "es": "Conservación de versiones y correcciones",
                "pt": "Conservação de versões e correções",
                "ar": "حفظ النسخ والتصحيحات"
              },
              "type": "single",
              "options": [
                {
                  "code": 1,
                  "label": {
                    "fr": "Existe et a été utilisé pour ces appuis pendant la période",
                    "en": "Exists and was used for this support during the period",
                    "es": "Existe y se utilizó para estos apoyos durante el período",
                    "pt": "Existe e foi utilizado para estes apoios durante o período",
                    "ar": "موجود واستخدم لهذا الدعم خلال الفترة"
                  }
                },
                {
                  "code": 2,
                  "label": {
                    "fr": "Existe mais n’a pas été utilisé pendant la période",
                    "en": "Exists but was not used during the period",
                    "es": "Existe pero no se utilizó durante el período",
                    "pt": "Existe mas não foi utilizado durante o período",
                    "ar": "موجود لكنه لم يستخدم خلال الفترة"
                  }
                },
                {
                  "code": 0,
                  "label": {
                    "fr": "N’existe pas",
                    "en": "Does not exist",
                    "es": "No existe",
                    "pt": "Não existe",
                    "ar": "غير موجود"
                  }
                },
                {
                  "code": 98,
                  "label": {
                    "fr": "Ne sait pas",
                    "en": "Do not know",
                    "es": "No sabe",
                    "pt": "Não sabe",
                    "ar": "لا أعرف"
                  }
                },
                {
                  "code": 99,
                  "label": {
                    "fr": "Non applicable",
                    "en": "Not applicable",
                    "es": "No aplicable",
                    "pt": "Não aplicável",
                    "ar": "لا ينطبق"
                  }
                }
              ],
              "gate": {
                "all": [
                  {
                    "field": "QP01_Participation",
                    "op": "in",
                    "values": [
                      1
                    ]
                  },
                  {
                    "field": "QP01_Produit",
                    "op": "not_in",
                    "values": [
                      0
                    ]
                  }
                ]
              },
              "source": "Rapport méthodologique, annexe 1bis, pp. 23–27, QP09"
            },
            {
              "id": "QP09_D7",
              "label": {
                "fr": "Examen des délais, difficultés et retours des utilisateurs",
                "en": "Review of deadlines, difficulties and user feedback",
                "es": "Examen de plazos, dificultades y comentarios de usuarios",
                "pt": "Análise dos prazos, dificuldades e comentários dos utilizadores",
                "ar": "مراجعة المهل والصعوبات وملاحظات المستخدمين"
              },
              "type": "single",
              "options": [
                {
                  "code": 1,
                  "label": {
                    "fr": "Existe et a été utilisé pour ces appuis pendant la période",
                    "en": "Exists and was used for this support during the period",
                    "es": "Existe y se utilizó para estos apoyos durante el período",
                    "pt": "Existe e foi utilizado para estes apoios durante o período",
                    "ar": "موجود واستخدم لهذا الدعم خلال الفترة"
                  }
                },
                {
                  "code": 2,
                  "label": {
                    "fr": "Existe mais n’a pas été utilisé pendant la période",
                    "en": "Exists but was not used during the period",
                    "es": "Existe pero no se utilizó durante el período",
                    "pt": "Existe mas não foi utilizado durante o período",
                    "ar": "موجود لكنه لم يستخدم خلال الفترة"
                  }
                },
                {
                  "code": 0,
                  "label": {
                    "fr": "N’existe pas",
                    "en": "Does not exist",
                    "es": "No existe",
                    "pt": "Não existe",
                    "ar": "غير موجود"
                  }
                },
                {
                  "code": 98,
                  "label": {
                    "fr": "Ne sait pas",
                    "en": "Do not know",
                    "es": "No sabe",
                    "pt": "Não sabe",
                    "ar": "لا أعرف"
                  }
                },
                {
                  "code": 99,
                  "label": {
                    "fr": "Non applicable",
                    "en": "Not applicable",
                    "es": "No aplicable",
                    "pt": "Não aplicável",
                    "ar": "لا ينطبق"
                  }
                }
              ],
              "gate": {
                "all": [
                  {
                    "field": "QP01_Participation",
                    "op": "in",
                    "values": [
                      1
                    ]
                  },
                  {
                    "field": "QP01_Produit",
                    "op": "not_in",
                    "values": [
                      0
                    ]
                  }
                ]
              },
              "source": "Rapport méthodologique, annexe 1bis, pp. 23–27, QP09"
            },
            {
              "id": "QP09_Nouveaute",
              "label": {
                "fr": "Pratique nouvelle après le 31 octobre, facultatif : dispositif, date de mise en place et une phrase",
                "en": "New practice after 31 October, optional: arrangement, implementation date and one sentence",
                "es": "Práctica nueva después del 31 de octubre, opcional : dispositivo, fecha de implantación y una frase",
                "pt": "Prática nova após 31 de outubro, facultativo : dispositivo, data de implementação e uma frase",
                "ar": "ممارسة جديدة بعد 31 أكتوبر، اختياري : الترتيب وتاريخ تطبيقه وجملة توضيحية"
              },
              "type": "textarea",
              "options": [],
              "max_chars": 2500,
              "gate": {
                "all": [
                  {
                    "field": "QP01_Participation",
                    "op": "in",
                    "values": [
                      1
                    ]
                  },
                  {
                    "field": "QP01_Produit",
                    "op": "not_in",
                    "values": [
                      0
                    ]
                  }
                ]
              },
              "source": "Rapport méthodologique, annexe 1bis, pp. 23–27, QP09"
            },
            {
              "id": "QP09b",
              "label": {
                "fr": "Quels échanges avec l’INS ont effectivement servi ces appuis pendant la période ?",
                "en": "Which exchanges with the NSO actually supported this work during the period?",
                "es": "¿Qué intercambios con la ONE contribuyeron efectivamente a estos apoyos durante el período?",
                "pt": "Que trocas com o INE contribuíram efetivamente para estes apoios durante o período?",
                "ar": "ما التبادلات مع الجهاز الوطني للإحصاء التي أسهمت فعليا في هذا الدعم خلال الفترة؟"
              },
              "type": "multi",
              "options": [
                {
                  "code": 1,
                  "label": {
                    "fr": "Orientation ou transmission des demandes",
                    "en": "Routing or forwarding requests",
                    "es": "Orientación o transmisión de solicitudes",
                    "pt": "Encaminhamento ou transmissão de pedidos",
                    "ar": "توجيه الطلبات أو إحالتها"
                  }
                },
                {
                  "code": 2,
                  "label": {
                    "fr": "Transmission de chiffres et métadonnées",
                    "en": "Providing figures and metadata",
                    "es": "Transmisión de cifras y metadatos",
                    "pt": "Transmissão de números e metadados",
                    "ar": "إرسال الأرقام والبيانات الوصفية"
                  }
                },
                {
                  "code": 3,
                  "label": {
                    "fr": "Harmonisation de définitions ou méthodes",
                    "en": "Harmonising definitions or methods",
                    "es": "Armonización de definiciones o métodos",
                    "pt": "Harmonização de definições ou métodos",
                    "ar": "مواءمة التعريفات أو المنهجيات"
                  }
                },
                {
                  "code": 4,
                  "label": {
                    "fr": "Vérification d’une valeur ou comparaison",
                    "en": "Checking a value or comparison",
                    "es": "Comprobación de un valor o comparación",
                    "pt": "Verificação de um valor ou comparação",
                    "ar": "التحقق من قيمة أو مقارنة"
                  }
                },
                {
                  "code": 5,
                  "label": {
                    "fr": "Coordination des corrections ou versions",
                    "en": "Coordinating corrections or versions",
                    "es": "Coordinación de correcciones o versiones",
                    "pt": "Coordenação de correções ou versões",
                    "ar": "تنسيق التصحيحات أو النسخ"
                  }
                },
                {
                  "code": 6,
                  "label": {
                    "fr": "Formation ou préparation de modèles",
                    "en": "Training or preparing templates",
                    "es": "Formación o preparación de modelos",
                    "pt": "Formação ou preparação de modelos",
                    "ar": "التدريب أو إعداد النماذج"
                  }
                },
                {
                  "code": 0,
                  "label": {
                    "fr": "Aucun échange connu",
                    "en": "No known exchange",
                    "es": "Ningún intercambio conocido",
                    "pt": "Nenhuma troca conhecida",
                    "ar": "لا أعرف أي تبادل"
                  }
                },
                {
                  "code": 97,
                  "label": {
                    "fr": "Autre",
                    "en": "Other",
                    "es": "Otra",
                    "pt": "Outra",
                    "ar": "أخرى"
                  }
                },
                {
                  "code": 98,
                  "label": {
                    "fr": "Ne sait pas",
                    "en": "Do not know",
                    "es": "No sabe",
                    "pt": "Não sabe",
                    "ar": "لا أعرف"
                  }
                },
                {
                  "code": 99,
                  "label": {
                    "fr": "Aucun appui concerné",
                    "en": "No relevant support",
                    "es": "Ningún apoyo pertinente",
                    "pt": "Nenhum apoio em causa",
                    "ar": "لا يوجد دعم معني"
                  }
                }
              ],
              "exclusive": [
                0,
                98,
                99
              ],
              "gate": {
                "all": [
                  {
                    "field": "QP01_Participation",
                    "op": "in",
                    "values": [
                      1
                    ]
                  },
                  {
                    "field": "QP01_Produit",
                    "op": "not_in",
                    "values": [
                      0
                    ]
                  }
                ]
              },
              "source": "Rapport méthodologique, annexe 1bis, pp. 23–27, QP09"
            },
            {
              "id": "QP09b_Autre",
              "label": {
                "fr": "Autre : précisez",
                "en": "Other: please specify",
                "es": "Otra : especifique",
                "pt": "Outra : especifique",
                "ar": "أخرى : يرجى التحديد"
              },
              "type": "text",
              "options": [],
              "max_chars": 500,
              "gate": {
                "all": [
                  {
                    "field": "QP01_Participation",
                    "op": "in",
                    "values": [
                      1
                    ]
                  },
                  {
                    "all": [
                      {
                        "field": "QP01_Produit",
                        "op": "not_in",
                        "values": [
                          0
                        ]
                      },
                      {
                        "field": "QP09b",
                        "op": "contains",
                        "values": [
                          97
                        ]
                      }
                    ]
                  }
                ]
              },
              "source": "Rapport méthodologique, annexe 1bis, pp. 23–27, QP09"
            },
            {
              "id": "QP09c",
              "label": {
                "fr": "Des divergences entre vos chiffres et ceux d’un autre producteur ont-elles nécessité une clarification pour un discours ?",
                "en": "Did discrepancies between your figures and another producer’s figures require clarification for a speech?",
                "es": "¿Exigieron aclaración para un discurso divergencias entre sus cifras y las de otro productor?",
                "pt": "Divergências entre os seus números e os de outro produtor exigiram esclarecimento para um discurso?",
                "ar": "هل استلزمت اختلافات بين أرقامكم وأرقام منتج آخر توضيحا من أجل خطاب؟"
              },
              "type": "single",
              "options": [
                {
                  "code": 1,
                  "label": {
                    "fr": "Oui",
                    "en": "Yes",
                    "es": "Sí",
                    "pt": "Sim",
                    "ar": "نعم"
                  }
                },
                {
                  "code": 0,
                  "label": {
                    "fr": "Aucun cas connu",
                    "en": "No known case",
                    "es": "Ningún caso conocido",
                    "pt": "Nenhum caso conhecido",
                    "ar": "لا أعرف أي حالة"
                  }
                },
                {
                  "code": 98,
                  "label": {
                    "fr": "Ne sait pas",
                    "en": "Do not know",
                    "es": "No sabe",
                    "pt": "Não sabe",
                    "ar": "لا أعرف"
                  }
                },
                {
                  "code": 99,
                  "label": {
                    "fr": "Aucun appui concerné",
                    "en": "No relevant support",
                    "es": "Ningún apoyo pertinente",
                    "pt": "Nenhum apoio em causa",
                    "ar": "لا يوجد دعم معني"
                  }
                }
              ],
              "gate": {
                "all": [
                  {
                    "field": "QP01_Participation",
                    "op": "in",
                    "values": [
                      1
                    ]
                  },
                  {
                    "field": "QP01_Produit",
                    "op": "not_in",
                    "values": [
                      0
                    ]
                  }
                ]
              },
              "source": "Rapport méthodologique, annexe 1bis, pp. 23–27, QP09"
            },
            {
              "id": "QP09c_Precision",
              "label": {
                "fr": "Type de divergence et traitement, en une phrase sans information confidentielle",
                "en": "Type of discrepancy and how it was addressed, in one sentence without confidential information",
                "es": "Tipo de divergencia y tratamiento, en una frase sin información confidencial",
                "pt": "Tipo de divergência e tratamento, numa frase sem informação confidencial",
                "ar": "نوع الاختلاف وكيف عولج، في جملة دون معلومات سرية"
              },
              "type": "textarea",
              "options": [],
              "max_chars": 2500,
              "gate": {
                "all": [
                  {
                    "field": "QP01_Participation",
                    "op": "in",
                    "values": [
                      1
                    ]
                  },
                  {
                    "all": [
                      {
                        "field": "QP01_Produit",
                        "op": "not_in",
                        "values": [
                          0
                        ]
                      },
                      {
                        "field": "QP09c",
                        "op": "in",
                        "values": [
                          1
                        ]
                      }
                    ]
                  }
                ]
              },
              "source": "Rapport méthodologique, annexe 1bis, pp. 23–27, QP09"
            }
          ],
          "note": {
            "fr": "",
            "en": "",
            "es": "",
            "pt": "",
            "ar": ""
          },
          "gate": null,
          "theme": "Suivi et moyens",
          "source": "Rapport méthodologique, annexe 1bis, pp. 23–27, QP09"
        },
        {
          "id": "QP10",
          "title": {
            "fr": "Besoins prioritaires",
            "en": "Priority needs",
            "es": "Necesidades prioritarias",
            "pt": "Necessidades prioritárias",
            "ar": "الاحتياجات ذات الأولوية"
          },
          "fields": [
            {
              "id": "QP10_P1",
              "label": {
                "fr": "Priorité 1",
                "en": "Priority 1",
                "es": "Prioridad 1",
                "pt": "Prioridade 1",
                "ar": "الأولوية 1"
              },
              "type": "single",
              "options": [
                {
                  "code": 1,
                  "label": {
                    "fr": "Clarifier le point d’entrée et les responsabilités",
                    "en": "Clarify the contact point and responsibilities",
                    "es": "Aclarar el punto de contacto y las responsabilidades",
                    "pt": "Clarificar o ponto de contacto e as responsabilidades",
                    "ar": "توضيح نقطة الاتصال والمسؤوليات"
                  }
                },
                {
                  "code": 2,
                  "label": {
                    "fr": "Mieux préciser et prioriser les demandes urgentes",
                    "en": "Clarify and prioritise urgent requests",
                    "es": "Precisar y priorizar las solicitudes urgentes",
                    "pt": "Precisar e priorizar os pedidos urgentes",
                    "ar": "تحديد الطلبات العاجلة وترتيب أولوياتها"
                  }
                },
                {
                  "code": 3,
                  "label": {
                    "fr": "Disposer d’un répertoire de chiffres documentés",
                    "en": "Have a repository of documented figures",
                    "es": "Disponer de un repositorio de cifras documentadas",
                    "pt": "Dispor de um repositório de números documentados",
                    "ar": "توفير دليل أرقام موثقة"
                  }
                },
                {
                  "code": 4,
                  "label": {
                    "fr": "Simplifier le contrôle et la validation",
                    "en": "Simplify checking and validation",
                    "es": "Simplificar el control y la validación",
                    "pt": "Simplificar o controlo e a validação",
                    "ar": "تبسيط المراجعة والتحقق"
                  }
                },
                {
                  "code": 5,
                  "label": {
                    "fr": "Préparer des messages et exemples de passages de discours",
                    "en": "Prepare messages and examples of speech passages",
                    "es": "Preparar mensajes y ejemplos de pasajes de discursos",
                    "pt": "Preparar mensagens e exemplos de trechos de discursos",
                    "ar": "إعداد رسائل وأمثلة لمقاطع الخطابات"
                  }
                },
                {
                  "code": 6,
                  "label": {
                    "fr": "Organiser les corrections et versions",
                    "en": "Organise corrections and versions",
                    "es": "Organizar las correcciones y las versiones",
                    "pt": "Organizar as correções e as versões",
                    "ar": "تنظيم التصحيحات والنسخ"
                  }
                },
                {
                  "code": 7,
                  "label": {
                    "fr": "Former les agents",
                    "en": "Train staff",
                    "es": "Formar al personal",
                    "pt": "Formar os agentes",
                    "ar": "تدريب الموظفين"
                  }
                },
                {
                  "code": 8,
                  "label": {
                    "fr": "Suivre les délais et les retours d’utilisation",
                    "en": "Monitor deadlines and user feedback",
                    "es": "Seguir los plazos y los comentarios sobre el uso",
                    "pt": "Acompanhar os prazos e os comentários sobre a utilização",
                    "ar": "متابعة المهل والملاحظات حول الاستخدام"
                  }
                },
                {
                  "code": 97,
                  "label": {
                    "fr": "Autre",
                    "en": "Other",
                    "es": "Otra",
                    "pt": "Outra",
                    "ar": "أخرى"
                  }
                },
                {
                  "code": 0,
                  "label": {
                    "fr": "Aucun besoin exprimé",
                    "en": "No need expressed",
                    "es": "Ninguna necesidad expresada",
                    "pt": "Nenhuma necessidade expressa",
                    "ar": "لم يعبر عن أي احتياج"
                  }
                },
                {
                  "code": 98,
                  "label": {
                    "fr": "Ne sait pas",
                    "en": "Do not know",
                    "es": "No sabe",
                    "pt": "Não sabe",
                    "ar": "لا أعرف"
                  }
                },
                {
                  "code": 9,
                  "label": {
                    "fr": "Améliorer la coordination avec l’INS et les autres producteurs",
                    "en": "Improve coordination with the NSO and other producers",
                    "es": "Mejorar la coordinación con la ONE y los demás productores",
                    "pt": "Melhorar a coordenação com o INE e os outros produtores",
                    "ar": "تحسين التنسيق مع الجهاز الوطني للإحصاء والمنتجين الآخرين"
                  }
                },
                {
                  "code": 10,
                  "label": {
                    "fr": "Clarifier la qualité et les limites des sources administratives",
                    "en": "Clarify the quality and limitations of administrative sources",
                    "es": "Aclarar la calidad y las limitaciones de las fuentes administrativas",
                    "pt": "Clarificar a qualidade e as limitações das fontes administrativas",
                    "ar": "توضيح جودة المصادر الإدارية وقيودها"
                  }
                }
              ],
              "gate": {
                "all": [
                  {
                    "field": "QP01_Participation",
                    "op": "in",
                    "values": [
                      1
                    ]
                  },
                  {
                    "field": "QP01_Produit",
                    "op": "not_in",
                    "values": [
                      0
                    ]
                  }
                ]
              },
              "source": "Rapport méthodologique, annexe 1bis, pp. 23–27, QP10"
            },
            {
              "id": "QP10_P2",
              "label": {
                "fr": "Priorité 2",
                "en": "Priority 2",
                "es": "Prioridad 2",
                "pt": "Prioridade 2",
                "ar": "الأولوية 2"
              },
              "type": "single",
              "options": [
                {
                  "code": 1,
                  "label": {
                    "fr": "Clarifier le point d’entrée et les responsabilités",
                    "en": "Clarify the contact point and responsibilities",
                    "es": "Aclarar el punto de contacto y las responsabilidades",
                    "pt": "Clarificar o ponto de contacto e as responsabilidades",
                    "ar": "توضيح نقطة الاتصال والمسؤوليات"
                  }
                },
                {
                  "code": 2,
                  "label": {
                    "fr": "Mieux préciser et prioriser les demandes urgentes",
                    "en": "Clarify and prioritise urgent requests",
                    "es": "Precisar y priorizar las solicitudes urgentes",
                    "pt": "Precisar e priorizar os pedidos urgentes",
                    "ar": "تحديد الطلبات العاجلة وترتيب أولوياتها"
                  }
                },
                {
                  "code": 3,
                  "label": {
                    "fr": "Disposer d’un répertoire de chiffres documentés",
                    "en": "Have a repository of documented figures",
                    "es": "Disponer de un repositorio de cifras documentadas",
                    "pt": "Dispor de um repositório de números documentados",
                    "ar": "توفير دليل أرقام موثقة"
                  }
                },
                {
                  "code": 4,
                  "label": {
                    "fr": "Simplifier le contrôle et la validation",
                    "en": "Simplify checking and validation",
                    "es": "Simplificar el control y la validación",
                    "pt": "Simplificar o controlo e a validação",
                    "ar": "تبسيط المراجعة والتحقق"
                  }
                },
                {
                  "code": 5,
                  "label": {
                    "fr": "Préparer des messages et exemples de passages de discours",
                    "en": "Prepare messages and examples of speech passages",
                    "es": "Preparar mensajes y ejemplos de pasajes de discursos",
                    "pt": "Preparar mensagens e exemplos de trechos de discursos",
                    "ar": "إعداد رسائل وأمثلة لمقاطع الخطابات"
                  }
                },
                {
                  "code": 6,
                  "label": {
                    "fr": "Organiser les corrections et versions",
                    "en": "Organise corrections and versions",
                    "es": "Organizar las correcciones y las versiones",
                    "pt": "Organizar as correções e as versões",
                    "ar": "تنظيم التصحيحات والنسخ"
                  }
                },
                {
                  "code": 7,
                  "label": {
                    "fr": "Former les agents",
                    "en": "Train staff",
                    "es": "Formar al personal",
                    "pt": "Formar os agentes",
                    "ar": "تدريب الموظفين"
                  }
                },
                {
                  "code": 8,
                  "label": {
                    "fr": "Suivre les délais et les retours d’utilisation",
                    "en": "Monitor deadlines and user feedback",
                    "es": "Seguir los plazos y los comentarios sobre el uso",
                    "pt": "Acompanhar os prazos e os comentários sobre a utilização",
                    "ar": "متابعة المهل والملاحظات حول الاستخدام"
                  }
                },
                {
                  "code": 97,
                  "label": {
                    "fr": "Autre",
                    "en": "Other",
                    "es": "Otra",
                    "pt": "Outra",
                    "ar": "أخرى"
                  }
                },
                {
                  "code": 9,
                  "label": {
                    "fr": "Améliorer la coordination avec l’INS et les autres producteurs",
                    "en": "Improve coordination with the NSO and other producers",
                    "es": "Mejorar la coordinación con la ONE y los demás productores",
                    "pt": "Melhorar a coordenação com o INE e os outros produtores",
                    "ar": "تحسين التنسيق مع الجهاز الوطني للإحصاء والمنتجين الآخرين"
                  }
                },
                {
                  "code": 10,
                  "label": {
                    "fr": "Clarifier la qualité et les limites des sources administratives",
                    "en": "Clarify the quality and limitations of administrative sources",
                    "es": "Aclarar la calidad y las limitaciones de las fuentes administrativas",
                    "pt": "Clarificar a qualidade e as limitações das fontes administrativas",
                    "ar": "توضيح جودة المصادر الإدارية وقيودها"
                  }
                }
              ],
              "gate": {
                "all": [
                  {
                    "field": "QP01_Participation",
                    "op": "in",
                    "values": [
                      1
                    ]
                  },
                  {
                    "all": [
                      {
                        "field": "QP01_Produit",
                        "op": "not_in",
                        "values": [
                          0
                        ]
                      },
                      {
                        "field": "QP10_P1",
                        "op": "in",
                        "values": [
                          1,
                          2,
                          3,
                          4,
                          5,
                          6,
                          7,
                          8,
                          97,
                          9,
                          10
                        ]
                      }
                    ]
                  }
                ]
              },
              "source": "Rapport méthodologique, annexe 1bis, pp. 23–27, QP10"
            },
            {
              "id": "QP10_P3",
              "label": {
                "fr": "Priorité 3",
                "en": "Priority 3",
                "es": "Prioridad 3",
                "pt": "Prioridade 3",
                "ar": "الأولوية 3"
              },
              "type": "single",
              "options": [
                {
                  "code": 1,
                  "label": {
                    "fr": "Clarifier le point d’entrée et les responsabilités",
                    "en": "Clarify the contact point and responsibilities",
                    "es": "Aclarar el punto de contacto y las responsabilidades",
                    "pt": "Clarificar o ponto de contacto e as responsabilidades",
                    "ar": "توضيح نقطة الاتصال والمسؤوليات"
                  }
                },
                {
                  "code": 2,
                  "label": {
                    "fr": "Mieux préciser et prioriser les demandes urgentes",
                    "en": "Clarify and prioritise urgent requests",
                    "es": "Precisar y priorizar las solicitudes urgentes",
                    "pt": "Precisar e priorizar os pedidos urgentes",
                    "ar": "تحديد الطلبات العاجلة وترتيب أولوياتها"
                  }
                },
                {
                  "code": 3,
                  "label": {
                    "fr": "Disposer d’un répertoire de chiffres documentés",
                    "en": "Have a repository of documented figures",
                    "es": "Disponer de un repositorio de cifras documentadas",
                    "pt": "Dispor de um repositório de números documentados",
                    "ar": "توفير دليل أرقام موثقة"
                  }
                },
                {
                  "code": 4,
                  "label": {
                    "fr": "Simplifier le contrôle et la validation",
                    "en": "Simplify checking and validation",
                    "es": "Simplificar el control y la validación",
                    "pt": "Simplificar o controlo e a validação",
                    "ar": "تبسيط المراجعة والتحقق"
                  }
                },
                {
                  "code": 5,
                  "label": {
                    "fr": "Préparer des messages et exemples de passages de discours",
                    "en": "Prepare messages and examples of speech passages",
                    "es": "Preparar mensajes y ejemplos de pasajes de discursos",
                    "pt": "Preparar mensagens e exemplos de trechos de discursos",
                    "ar": "إعداد رسائل وأمثلة لمقاطع الخطابات"
                  }
                },
                {
                  "code": 6,
                  "label": {
                    "fr": "Organiser les corrections et versions",
                    "en": "Organise corrections and versions",
                    "es": "Organizar las correcciones y las versiones",
                    "pt": "Organizar as correções e as versões",
                    "ar": "تنظيم التصحيحات والنسخ"
                  }
                },
                {
                  "code": 7,
                  "label": {
                    "fr": "Former les agents",
                    "en": "Train staff",
                    "es": "Formar al personal",
                    "pt": "Formar os agentes",
                    "ar": "تدريب الموظفين"
                  }
                },
                {
                  "code": 8,
                  "label": {
                    "fr": "Suivre les délais et les retours d’utilisation",
                    "en": "Monitor deadlines and user feedback",
                    "es": "Seguir los plazos y los comentarios sobre el uso",
                    "pt": "Acompanhar os prazos e os comentários sobre a utilização",
                    "ar": "متابعة المهل والملاحظات حول الاستخدام"
                  }
                },
                {
                  "code": 97,
                  "label": {
                    "fr": "Autre",
                    "en": "Other",
                    "es": "Otra",
                    "pt": "Outra",
                    "ar": "أخرى"
                  }
                },
                {
                  "code": 9,
                  "label": {
                    "fr": "Améliorer la coordination avec l’INS et les autres producteurs",
                    "en": "Improve coordination with the NSO and other producers",
                    "es": "Mejorar la coordinación con la ONE y los demás productores",
                    "pt": "Melhorar a coordenação com o INE e os outros produtores",
                    "ar": "تحسين التنسيق مع الجهاز الوطني للإحصاء والمنتجين الآخرين"
                  }
                },
                {
                  "code": 10,
                  "label": {
                    "fr": "Clarifier la qualité et les limites des sources administratives",
                    "en": "Clarify the quality and limitations of administrative sources",
                    "es": "Aclarar la calidad y las limitaciones de las fuentes administrativas",
                    "pt": "Clarificar a qualidade e as limitações das fontes administrativas",
                    "ar": "توضيح جودة المصادر الإدارية وقيودها"
                  }
                }
              ],
              "gate": {
                "all": [
                  {
                    "field": "QP01_Participation",
                    "op": "in",
                    "values": [
                      1
                    ]
                  },
                  {
                    "all": [
                      {
                        "field": "QP01_Produit",
                        "op": "not_in",
                        "values": [
                          0
                        ]
                      },
                      {
                        "field": "QP10_P1",
                        "op": "in",
                        "values": [
                          1,
                          2,
                          3,
                          4,
                          5,
                          6,
                          7,
                          8,
                          97,
                          9,
                          10
                        ]
                      }
                    ]
                  }
                ]
              },
              "source": "Rapport méthodologique, annexe 1bis, pp. 23–27, QP10"
            },
            {
              "id": "QP10_Autre",
              "label": {
                "fr": "Autre priorité : précisez",
                "en": "Other priority: please specify",
                "es": "Otra prioridad : especifique",
                "pt": "Outra prioridade : especifique",
                "ar": "أولوية أخرى : يرجى التحديد"
              },
              "type": "text",
              "options": [],
              "max_chars": 500,
              "gate": {
                "all": [
                  {
                    "field": "QP01_Participation",
                    "op": "in",
                    "values": [
                      1
                    ]
                  },
                  {
                    "all": [
                      {
                        "field": "QP01_Produit",
                        "op": "not_in",
                        "values": [
                          0
                        ]
                      },
                      {
                        "any": [
                          {
                            "field": "QP10_P1",
                            "op": "in",
                            "values": [
                              97
                            ]
                          },
                          {
                            "field": "QP10_P2",
                            "op": "in",
                            "values": [
                              97
                            ]
                          },
                          {
                            "field": "QP10_P3",
                            "op": "in",
                            "values": [
                              97
                            ]
                          }
                        ]
                      }
                    ]
                  }
                ]
              },
              "source": "Rapport méthodologique, annexe 1bis, pp. 23–27, QP10"
            },
            {
              "id": "QP10_Difficulte",
              "label": {
                "fr": "Pour la priorité 1, quelle difficulté réelle cela permettrait-il de résoudre ? Au plus 40 mots ; sinon, écrire « besoin anticipé ».",
                "en": "For priority 1, what actual difficulty would this resolve? Up to 40 words; otherwise write ‘anticipated need’.",
                "es": "Para la prioridad 1, ¿qué dificultad real se resolvería? Máximo 40 palabras; en su defecto, escriba « necesidad prevista ».",
                "pt": "Para a prioridade 1, que dificuldade real seria resolvida? Máximo de 40 palavras; caso contrário, escreva « necessidade prevista ».",
                "ar": "بالنسبة للأولوية 1، ما الصعوبة الفعلية التي ستحل؟ في حدود 40 كلمة، أو اكتب « احتياج متوقع »."
              },
              "type": "textarea",
              "options": [],
              "max_chars": 2500,
              "max_words": 40,
              "gate": {
                "all": [
                  {
                    "field": "QP01_Participation",
                    "op": "in",
                    "values": [
                      1
                    ]
                  },
                  {
                    "field": "QP01_Produit",
                    "op": "not_in",
                    "values": [
                      0
                    ]
                  }
                ]
              },
              "source": "Rapport méthodologique, annexe 1bis, pp. 23–27, QP10"
            },
            {
              "id": "QP10_Action",
              "label": {
                "fr": "Première action possible avec les moyens existants",
                "en": "First action possible with existing resources",
                "es": "Primera acción posible con los recursos existentes",
                "pt": "Primeira ação possível com os recursos existentes",
                "ar": "أول إجراء ممكن بالموارد المتاحة"
              },
              "type": "text",
              "options": [],
              "max_chars": 500,
              "gate": {
                "all": [
                  {
                    "field": "QP01_Participation",
                    "op": "in",
                    "values": [
                      1
                    ]
                  },
                  {
                    "field": "QP01_Produit",
                    "op": "not_in",
                    "values": [
                      0
                    ]
                  }
                ]
              },
              "source": "Rapport méthodologique, annexe 1bis, pp. 23–27, QP10"
            },
            {
              "id": "QP10_Responsable",
              "label": {
                "fr": "Fonction pouvant la conduire",
                "en": "Function that could lead the action",
                "es": "Función que podría dirigir la acción",
                "pt": "Função que poderia conduzir a ação",
                "ar": "الوظيفة التي يمكن أن تقود الإجراء"
              },
              "type": "text",
              "options": [],
              "max_chars": 500,
              "gate": {
                "all": [
                  {
                    "field": "QP01_Participation",
                    "op": "in",
                    "values": [
                      1
                    ]
                  },
                  {
                    "field": "QP01_Produit",
                    "op": "not_in",
                    "values": [
                      0
                    ]
                  }
                ]
              },
              "source": "Rapport méthodologique, annexe 1bis, pp. 23–27, QP10"
            },
            {
              "id": "QP10_Moyen",
              "label": {
                "fr": "Moyen déjà disponible",
                "en": "Resource already available",
                "es": "Recurso ya disponible",
                "pt": "Recurso já disponível",
                "ar": "المورد المتاح بالفعل"
              },
              "type": "text",
              "options": [],
              "max_chars": 500,
              "gate": {
                "all": [
                  {
                    "field": "QP01_Participation",
                    "op": "in",
                    "values": [
                      1
                    ]
                  },
                  {
                    "field": "QP01_Produit",
                    "op": "not_in",
                    "values": [
                      0
                    ]
                  }
                ]
              },
              "source": "Rapport méthodologique, annexe 1bis, pp. 23–27, QP10"
            },
            {
              "id": "QP10_Obstacle",
              "label": {
                "fr": "Principal obstacle",
                "en": "Main obstacle",
                "es": "Principal obstáculo",
                "pt": "Principal obstáculo",
                "ar": "العائق الرئيسي"
              },
              "type": "text",
              "options": [],
              "max_chars": 500,
              "gate": {
                "all": [
                  {
                    "field": "QP01_Participation",
                    "op": "in",
                    "values": [
                      1
                    ]
                  },
                  {
                    "field": "QP01_Produit",
                    "op": "not_in",
                    "values": [
                      0
                    ]
                  }
                ]
              },
              "source": "Rapport méthodologique, annexe 1bis, pp. 23–27, QP10"
            }
          ],
          "note": {
            "fr": "Classer au plus trois codes différents. Une ou deux priorités suffisent. 0 et 98 sont exclusifs des autres priorités. Indiquer « à déterminer » si nécessaire pour l’action, le responsable, les moyens ou l’obstacle.",
            "en": "Rank up to three different codes. One or two priorities are sufficient. Codes 0 and 98 exclude other priorities. Write ‘to be determined’ where necessary for the action, lead function, resources or obstacle.",
            "es": "Clasifique hasta tres códigos distintos. Una o dos prioridades bastan. Los códigos 0 y 98 excluyen las demás prioridades. Escriba « por determinar » cuando sea necesario para la acción, función responsable, recursos u obstáculo.",
            "pt": "Ordene até três códigos distintos. Uma ou duas prioridades bastam. Os códigos 0 e 98 excluem as restantes prioridades. Escreva « a determinar » quando necessário para a ação, função responsável, recursos ou obstáculo.",
            "ar": "رتب ثلاثة رموز مختلفة كحد أقصى. تكفي أولوية أو أولويتان. يستبعد الرمزان 0 و98 بقية الأولويات. اكتب « يحدد لاحقا » عند الحاجة للإجراء أو الوظيفة المسؤولة أو الموارد أو العائق."
          },
          "gate": null,
          "theme": "Suivi et moyens",
          "source": "Rapport méthodologique, annexe 1bis, pp. 23–27, QP10"
        },
        {
          "id": "QP11",
          "title": {
            "fr": "Exemple partageable et lien avec la décision",
            "en": "Shareable example and link to decision-making",
            "es": "Ejemplo compartible y vínculo con la decisión",
            "pt": "Exemplo partilhável e ligação à decisão",
            "ar": "مثال قابل للمشاركة وصلته بالقرار"
          },
          "fields": [
            {
              "id": "QP11_Type",
              "label": {
                "fr": "Exemple proposé, facultatif",
                "en": "Example provided, optional",
                "es": "Ejemplo facilitado, opcional",
                "pt": "Exemplo fornecido, facultativo",
                "ar": "المثال المقدم، اختياري"
              },
              "type": "single",
              "options": [
                {
                  "code": 1,
                  "label": {
                    "fr": "Lien public",
                    "en": "Public link",
                    "es": "Enlace público",
                    "pt": "Ligação pública",
                    "ar": "رابط متاح للعموم"
                  }
                },
                {
                  "code": 2,
                  "label": {
                    "fr": "Extrait non confidentiel dont le partage est autorisé",
                    "en": "Non-confidential extract authorised for sharing",
                    "es": "Extracto no confidencial cuya difusión está autorizada",
                    "pt": "Excerto não confidencial cuja partilha está autorizada",
                    "ar": "مقتطف غير سري مصرح بمشاركته"
                  }
                },
                {
                  "code": 0,
                  "label": {
                    "fr": "Aucun exemple partagé",
                    "en": "No example shared",
                    "es": "No se comparte ningún ejemplo",
                    "pt": "Nenhum exemplo partilhado",
                    "ar": "لم يشارك أي مثال"
                  }
                }
              ],
              "gate": {
                "all": [
                  {
                    "field": "QP01_Participation",
                    "op": "in",
                    "values": [
                      1
                    ]
                  },
                  {
                    "field": "QP01_Produit",
                    "op": "not_in",
                    "values": [
                      0
                    ]
                  }
                ]
              },
              "source": "Rapport méthodologique, annexe 1bis, pp. 23–27, QP11"
            },
            {
              "id": "QP11_Reference",
              "label": {
                "fr": "Titre ou description, date et lien public si disponible ; extrait autorisé le cas échéant",
                "en": "Title or description, date and public link if available; authorised extract where relevant",
                "es": "Título o descripción, fecha y enlace público si existe; extracto autorizado, si procede",
                "pt": "Título ou descrição, data e ligação pública se disponível; excerto autorizado, se aplicável",
                "ar": "العنوان أو الوصف والتاريخ والرابط العام إن وجد، ومقتطف مصرح به عند الاقتضاء"
              },
              "type": "textarea",
              "options": [],
              "max_chars": 2500,
              "gate": {
                "all": [
                  {
                    "field": "QP01_Participation",
                    "op": "in",
                    "values": [
                      1
                    ]
                  },
                  {
                    "all": [
                      {
                        "field": "QP01_Produit",
                        "op": "not_in",
                        "values": [
                          0
                        ]
                      },
                      {
                        "field": "QP11_Type",
                        "op": "in",
                        "values": [
                          1,
                          2
                        ]
                      }
                    ]
                  }
                ]
              },
              "source": "Rapport méthodologique, annexe 1bis, pp. 23–27, QP11"
            },
            {
              "id": "QP11_Apport",
              "label": {
                "fr": "Ce que cet exemple montre et ce qui pourrait être adapté ailleurs, au plus 60 mots",
                "en": "What this example demonstrates and what could be adapted elsewhere, up to 60 words",
                "es": "Qué demuestra el ejemplo y qué podría adaptarse en otro contexto, hasta 60 palabras",
                "pt": "O que o exemplo demonstra e o que poderia ser adaptado noutro contexto, até 60 palavras",
                "ar": "ما الذي يوضحه المثال وما الذي يمكن تكييفه في سياق آخر، حتى 60 كلمة"
              },
              "type": "textarea",
              "options": [],
              "max_chars": 2500,
              "max_words": 60,
              "gate": {
                "all": [
                  {
                    "field": "QP01_Participation",
                    "op": "in",
                    "values": [
                      1
                    ]
                  },
                  {
                    "all": [
                      {
                        "field": "QP01_Produit",
                        "op": "not_in",
                        "values": [
                          0
                        ]
                      },
                      {
                        "field": "QP11_Type",
                        "op": "in",
                        "values": [
                          1,
                          2
                        ]
                      }
                    ]
                  }
                ]
              },
              "source": "Rapport méthodologique, annexe 1bis, pp. 23–27, QP11"
            },
            {
              "id": "QP11_Usage",
              "label": {
                "fr": "Usage illustré",
                "en": "Use illustrated",
                "es": "Uso ilustrado",
                "pt": "Utilização ilustrada",
                "ar": "الاستخدام الموضح"
              },
              "type": "single",
              "options": [
                {
                  "code": 1,
                  "label": {
                    "fr": "Discours",
                    "en": "Speech",
                    "es": "Discurso",
                    "pt": "Discurso",
                    "ar": "خطاب"
                  }
                },
                {
                  "code": 2,
                  "label": {
                    "fr": "Décision sans discours identifié",
                    "en": "Decision with no identified speech",
                    "es": "Decisión sin discurso identificado",
                    "pt": "Decisão sem discurso identificado",
                    "ar": "قرار دون خطاب محدد"
                  }
                },
                {
                  "code": 3,
                  "label": {
                    "fr": "Les deux",
                    "en": "Both",
                    "es": "Ambos",
                    "pt": "Ambos",
                    "ar": "كلاهما"
                  }
                },
                {
                  "code": 98,
                  "label": {
                    "fr": "Ne sait pas",
                    "en": "Do not know",
                    "es": "No sabe",
                    "pt": "Não sabe",
                    "ar": "لا أعرف"
                  }
                }
              ],
              "gate": {
                "all": [
                  {
                    "field": "QP01_Participation",
                    "op": "in",
                    "values": [
                      1
                    ]
                  },
                  {
                    "all": [
                      {
                        "field": "QP01_Produit",
                        "op": "not_in",
                        "values": [
                          0
                        ]
                      },
                      {
                        "field": "QP11_Type",
                        "op": "in",
                        "values": [
                          1,
                          2
                        ]
                      }
                    ]
                  }
                ]
              },
              "source": "Rapport méthodologique, annexe 1bis, pp. 23–27, QP11"
            },
            {
              "id": "QP11_Preuve",
              "label": {
                "fr": "Preuve d’utilisation",
                "en": "Evidence of use",
                "es": "Prueba de utilización",
                "pt": "Prova de utilização",
                "ar": "إثبات الاستخدام"
              },
              "type": "single",
              "options": [
                {
                  "code": 1,
                  "label": {
                    "fr": "Document accessible ou extrait autorisé",
                    "en": "Accessible document or authorised extract",
                    "es": "Documento accesible o extracto autorizado",
                    "pt": "Documento acessível ou excerto autorizado",
                    "ar": "وثيقة متاحة أو مقتطف مصرح به"
                  }
                },
                {
                  "code": 2,
                  "label": {
                    "fr": "Usage connu mais non documenté ici",
                    "en": "Known use, not documented here",
                    "es": "Uso conocido pero no documentado aquí",
                    "pt": "Utilização conhecida mas não documentada aqui",
                    "ar": "استخدام معروف لكنه غير موثق هنا"
                  }
                },
                {
                  "code": 98,
                  "label": {
                    "fr": "Ne sait pas",
                    "en": "Do not know",
                    "es": "No sabe",
                    "pt": "Não sabe",
                    "ar": "لا أعرف"
                  }
                }
              ],
              "gate": {
                "all": [
                  {
                    "field": "QP01_Participation",
                    "op": "in",
                    "values": [
                      1
                    ]
                  },
                  {
                    "all": [
                      {
                        "field": "QP01_Produit",
                        "op": "not_in",
                        "values": [
                          0
                        ]
                      },
                      {
                        "field": "QP11_Type",
                        "op": "in",
                        "values": [
                          1,
                          2
                        ]
                      }
                    ]
                  }
                ]
              },
              "source": "Rapport méthodologique, annexe 1bis, pp. 23–27, QP11"
            }
          ],
          "note": {
            "fr": "Facultatif : procédure, fiche chiffre, contrôle, correction, discours public ou note de décision autorisée. Ne pas partager de contenu confidentiel. Un exemple plus ancien conserve sa date et ne constitue pas une activité de la période. Une proposition d’usage ou un effet supposé ne prouve pas une utilisation réelle.",
            "en": "Optional: procedure, figure fact sheet, check, correction, public speech or authorised decision note. Do not share confidential content. An older example retains its date and is not activity during the reference period. A proposed use or assumed effect does not prove actual use.",
            "es": "Opcional : procedimiento, ficha de cifra, control, corrección, discurso público o nota de decisión autorizada. No comparta contenido confidencial. Un ejemplo antiguo conserva su fecha y no cuenta como actividad del período. Un uso propuesto o un efecto supuesto no prueba una utilización real.",
            "pt": "Facultativo : procedimento, ficha de número, controlo, correção, discurso público ou nota de decisão autorizada. Não partilhe conteúdo confidencial. Um exemplo antigo conserva a data e não conta como atividade do período. Uma utilização proposta ou um efeito suposto não prova uma utilização real.",
            "ar": "اختياري : إجراء أو بطاقة رقم أو مراجعة أو تصحيح أو خطاب عام أو مذكرة قرار مصرح بها. لا تشارك محتوى سريا. يحتفظ المثال القديم بتاريخه ولا يعد نشاطا خلال الفترة. الاستخدام المقترح أو الأثر المفترض لا يثبت استخداما فعليا."
          },
          "gate": null,
          "theme": "Suivi et moyens",
          "source": "Rapport méthodologique, annexe 1bis, pp. 23–27, QP11"
        },
        {
          "id": "QP12",
          "title": {
            "fr": "Attribution et contact facultatif",
            "en": "Attribution and optional contact",
            "es": "Atribución y contacto opcional",
            "pt": "Atribuição e contacto facultativo",
            "ar": "الإسناد وبيانات الاتصال الاختيارية"
          },
          "fields": [
            {
              "id": "QP12_Attribution",
              "label": {
                "fr": "Modalité autorisée pour la contribution et l’exemple partagé",
                "en": "Permitted use of the contribution and shared example",
                "es": "Uso autorizado de la contribución y del ejemplo compartido",
                "pt": "Utilização autorizada da contribuição e do exemplo partilhado",
                "ar": "الاستخدام المصرح به للمساهمة والمثال المشترك"
              },
              "type": "single",
              "options": [
                {
                  "code": 0,
                  "label": {
                    "fr": "Synthèse anonymisée seulement, sans attribution identifiable",
                    "en": "Anonymised summary only, with no identifiable attribution",
                    "es": "Solo síntesis anonimizada, sin atribución identificable",
                    "pt": "Apenas síntese anonimizada, sem atribuição identificável",
                    "ar": "ملخص دون تحديد الهوية فقط، دون إسناد كاشف للهوية"
                  }
                },
                {
                  "code": 1,
                  "label": {
                    "fr": "Attribution à l’institution, sans citation textuelle identifiable",
                    "en": "Attribution to the institution, without an identifiable quotation",
                    "es": "Atribución a la institución, sin cita textual identificable",
                    "pt": "Atribuição à instituição, sem citação textual identificável",
                    "ar": "الإسناد إلى المؤسسة دون اقتباس نصي كاشف للهوية"
                  }
                },
                {
                  "code": 2,
                  "label": {
                    "fr": "Attribution à l’institution et citation textuelle",
                    "en": "Attribution to the institution and direct quotation",
                    "es": "Atribución a la institución y cita textual",
                    "pt": "Atribuição à instituição e citação textual",
                    "ar": "الإسناد إلى المؤسسة والاقتباس النصي"
                  }
                },
                {
                  "code": 3,
                  "label": {
                    "fr": "Me soumettre le passage avant toute attribution ou citation identifiable",
                    "en": "Submit the passage to me before any identifiable attribution or quotation",
                    "es": "Enviarme el pasaje antes de cualquier atribución o cita identificable",
                    "pt": "Enviar-me o trecho antes de qualquer atribuição ou citação identificável",
                    "ar": "اعرض علي المقطع قبل أي إسناد أو اقتباس كاشف للهوية"
                  }
                }
              ],
              "attribution": true,
              "gate": {
                "all": [
                  {
                    "field": "QP01_Participation",
                    "op": "in",
                    "values": [
                      1
                    ]
                  },
                  {
                    "field": "QP01_Produit",
                    "op": "not_in",
                    "values": [
                      0
                    ]
                  }
                ]
              },
              "source": "Rapport méthodologique, annexe 1bis, pp. 23–27, QP12"
            },
            {
              "id": "QP12_Suite",
              "label": {
                "fr": "Accepteriez-vous un bref complément ou un test d’outil ?",
                "en": "Would you agree to a brief follow-up or a tool test?",
                "es": "¿Aceptaría una breve aclaración o una prueba de herramienta?",
                "pt": "Aceitaria um breve esclarecimento ou um teste de ferramenta?",
                "ar": "هل تقبل استكمالا موجزا أو اختبار أداة؟"
              },
              "type": "single",
              "options": [
                {
                  "code": 1,
                  "label": {
                    "fr": "Complément",
                    "en": "Follow-up clarification",
                    "es": "Complemento",
                    "pt": "Complemento",
                    "ar": "استكمال"
                  }
                },
                {
                  "code": 2,
                  "label": {
                    "fr": "Test",
                    "en": "Tool test",
                    "es": "Prueba de herramienta",
                    "pt": "Teste de ferramenta",
                    "ar": "اختبار أداة"
                  }
                },
                {
                  "code": 3,
                  "label": {
                    "fr": "Les deux",
                    "en": "Both",
                    "es": "Ambos",
                    "pt": "Ambos",
                    "ar": "كلاهما"
                  }
                },
                {
                  "code": 0,
                  "label": {
                    "fr": "Non",
                    "en": "No",
                    "es": "No",
                    "pt": "Não",
                    "ar": "لا"
                  }
                },
                {
                  "code": 98,
                  "label": {
                    "fr": "À déterminer",
                    "en": "To be determined",
                    "es": "Por determinar",
                    "pt": "A determinar",
                    "ar": "يحدد لاحقا"
                  }
                }
              ],
              "gate": {
                "all": [
                  {
                    "field": "QP01_Participation",
                    "op": "in",
                    "values": [
                      1
                    ]
                  },
                  {
                    "field": "QP01_Produit",
                    "op": "not_in",
                    "values": [
                      0
                    ]
                  }
                ]
              },
              "source": "Rapport méthodologique, annexe 1bis, pp. 23–27, QP12"
            },
            {
              "id": "QP12_Contact_fonction",
              "label": {
                "fr": "Contact facultatif : fonction ou service à recontacter",
                "en": "Optional contact: function or service to contact",
                "es": "Contacto opcional : función o servicio al que contactar",
                "pt": "Contacto facultativo : função ou serviço a contactar",
                "ar": "جهة اتصال اختيارية : الوظيفة أو الإدارة التي يمكن التواصل معها"
              },
              "type": "text",
              "options": [],
              "max_chars": 500,
              "private": true,
              "gate": {
                "all": [
                  {
                    "field": "QP01_Participation",
                    "op": "in",
                    "values": [
                      1
                    ]
                  },
                  {
                    "field": "QP01_Produit",
                    "op": "not_in",
                    "values": [
                      0
                    ]
                  }
                ]
              },
              "source": "Rapport méthodologique, annexe 1bis, pp. 23–27, QP12"
            },
            {
              "id": "QP12_Contact_courriel",
              "label": {
                "fr": "Courriel institutionnel ou générique, facultatif",
                "en": "Institutional or generic email address, optional",
                "es": "Correo institucional o genérico, opcional",
                "pt": "Correio institucional ou genérico, facultativo",
                "ar": "بريد مؤسسي أو عام، اختياري"
              },
              "type": "email",
              "options": [],
              "max_chars": 254,
              "private": true,
              "gate": {
                "all": [
                  {
                    "field": "QP01_Participation",
                    "op": "in",
                    "values": [
                      1
                    ]
                  },
                  {
                    "field": "QP01_Produit",
                    "op": "not_in",
                    "values": [
                      0
                    ]
                  }
                ]
              },
              "source": "Rapport méthodologique, annexe 1bis, pp. 23–27, QP12"
            },
            {
              "id": "QP12_Contact_relais",
              "label": {
                "fr": "Préférer un relais par STATAFRIC, facultatif",
                "en": "Prefer contact through STATAFRIC, optional",
                "es": "Preferir contacto a través de STATAFRIC, opcional",
                "pt": "Preferir contacto através do STATAFRIC, facultativo",
                "ar": "تفضيل التواصل عبر STATAFRIC، اختياري"
              },
              "type": "single",
              "options": [
                {
                  "code": 1,
                  "label": {
                    "fr": "Passer par STATAFRIC",
                    "en": "Contact through STATAFRIC",
                    "es": "Contacto a través de STATAFRIC",
                    "pt": "Contacto através do STATAFRIC",
                    "ar": "التواصل عبر STATAFRIC"
                  }
                },
                {
                  "code": 2,
                  "label": {
                    "fr": "J’autorise l’INS invitant à transmettre une demande de suivi, sans accès à ma contribution",
                    "en": "I authorise the inviting NSO to forward a follow-up request, without access to my contribution",
                    "es": "Autorizo a la ONE que me invitó a transmitir una solicitud de seguimiento, sin acceso a mi contribución",
                    "pt": "Autorizo o INE que me convidou a transmitir um pedido de seguimento, sem acesso à minha contribuição",
                    "ar": "أسمح للجهاز الوطني للإحصاء الداعي بإحالة طلب متابعة دون الاطلاع على مساهمتي"
                  }
                }
              ],
              "private": true,
              "gate": {
                "all": [
                  {
                    "field": "QP01_Participation",
                    "op": "in",
                    "values": [
                      1
                    ]
                  },
                  {
                    "field": "QP01_Produit",
                    "op": "not_in",
                    "values": [
                      0
                    ]
                  }
                ]
              },
              "source": "Rapport méthodologique, annexe 1bis, pp. 23–27, QP12"
            }
          ],
          "note": {
            "fr": "Les choix 1 et 2 supposent l’autorisation correspondante. Une contribution individuelle sans cette autorisation reste anonymisée. Sans choix explicite, la synthèse anonymisée (code 0) s’applique. L’absence de retour ne vaut jamais accord de citation. Le relais par l’INS ne remplace pas votre autorisation institutionnelle.",
            "en": "Options 1 and 2 require the corresponding authorisation. An individual contribution without that authorisation remains anonymised. Without an explicit choice, an anonymised summary (code 0) applies. Silence never constitutes permission to quote. Forwarding by the NSO does not replace your institutional authorisation.",
            "es": "Las opciones 1 y 2 exigen autorización. La contribución individual sin autorización permanece anonimizada. Sin elección explícita se aplica el código 0. El silencio nunca autoriza citas. La transmisión por la ONE no sustituye su autorización institucional.",
            "pt": "As opções 1 e 2 exigem autorização. A contribuição individual sem autorização mantém-se anonimizada. Sem escolha explícita aplica-se o código 0. O silêncio nunca autoriza citações. O encaminhamento pelo INE não substitui a sua autorização institucional.",
            "ar": "يتطلب الخياران 1 و2 التصريح المناسب. تبقى المساهمة الفردية دونه مجهولة الهوية. عند غياب اختيار صريح يطبق الرمز 0. الصمت لا يجيز الاقتباس. إحالة الجهاز الوطني للإحصاء لا تحل محل تصريح مؤسستكم."
          },
          "gate": null,
          "theme": "Suivi et moyens",
          "source": "Rapport méthodologique, annexe 1bis, pp. 23–27, QP12"
        }
      ],
      "consent": "QP01_Participation",
      "attribution": "QP12_Attribution",
      "institution": "QP01_Institution",
      "country": "QP01_Pays",
      "status": "QP01_Statut",
      "previous": "QP01_Reference_precedente",
      "version": "1.0 — 2026-11-15",
      "unit": "QP01_Unite",
      "sector": "QP01_Domaine",
      "response_language": "QP01_Langue_texte",
      "origin": "QP01_Origine_invitation",
      "relay": "QP01_INS_relais",
      "profile": "Autre producteur",
      "priority_prefix": "QP10"
    },
    "utilisateurs": {
      "title": {
        "fr": "Équipes préparant les discours",
        "en": "Speech-preparation teams",
        "es": "Equipos que preparan discursos",
        "pt": "Equipas que preparam discursos",
        "ar": "فرق إعداد الخطابات"
      },
      "duration": {
        "fr": "10 à 15 minutes",
        "en": "10 to 15 minutes",
        "es": "De 10 a 15 minutos",
        "pt": "10 a 15 minutos",
        "ar": "من 10 إلى 15 دقيقة"
      },
      "sections": [
        {
          "id": "QU01",
          "title": {
            "fr": "Accord de participation et profil",
            "en": "Participation agreement and profile",
            "es": "Consentimiento de participación y perfil",
            "pt": "Consentimento de participação e perfil",
            "ar": "الموافقة على المشاركة والصفة"
          },
          "fields": [
            {
              "id": "QU01_Participation",
              "label": {
                "fr": "Acceptez-vous l’utilisation de votre contribution selon la notice ?",
                "en": "Do you agree to the use of your contribution as described in the notice?",
                "es": "¿Autoriza el uso de su contribución según el aviso de participación?",
                "pt": "Autoriza a utilização da sua contribuição conforme o aviso de participação?",
                "ar": "هل توافق على استخدام مساهمتك وفق إشعار المشاركة؟"
              },
              "type": "single",
              "options": [
                {
                  "code": 1,
                  "label": {
                    "fr": "J’accepte",
                    "en": "I agree",
                    "es": "Acepto",
                    "pt": "Aceito",
                    "ar": "أوافق"
                  }
                },
                {
                  "code": 0,
                  "label": {
                    "fr": "Je ne participe pas — fin du questionnaire",
                    "en": "I do not wish to participate — end of questionnaire",
                    "es": "No deseo participar — fin del cuestionario",
                    "pt": "Não desejo participar — fim do questionário",
                    "ar": "لا أرغب في المشاركة — نهاية الاستبيان"
                  }
                }
              ],
              "required": true,
              "source": "Rapport méthodologique du 15 novembre 2026, pp. 28–30, QU01"
            },
            {
              "id": "QU01_Pays",
              "label": {
                "fr": "Pays",
                "en": "Country",
                "es": "País",
                "pt": "País",
                "ar": "البلد"
              },
              "type": "text",
              "options": [],
              "max_chars": 500,
              "source": "Rapport méthodologique du 15 novembre 2026, pp. 28–30, QU01",
              "gate": {
                "field": "QU01_Participation",
                "op": "in",
                "values": [
                  1
                ]
              }
            },
            {
              "id": "QU01_Institution",
              "label": {
                "fr": "Institution ou type d’institution",
                "en": "Institution or type of institution",
                "es": "Institución o tipo de institución",
                "pt": "Instituição ou tipo de instituição",
                "ar": "المؤسسة أو نوع المؤسسة"
              },
              "type": "text",
              "options": [],
              "max_chars": 500,
              "source": "Rapport méthodologique du 15 novembre 2026, pp. 28–30, QU01",
              "gate": {
                "field": "QU01_Participation",
                "op": "in",
                "values": [
                  1
                ]
              }
            },
            {
              "id": "QU01_Statut",
              "label": {
                "fr": "Périmètre",
                "en": "Scope",
                "es": "Ámbito",
                "pt": "Âmbito",
                "ar": "النطاق"
              },
              "type": "single",
              "options": [
                {
                  "code": 1,
                  "label": {
                    "fr": "Réponse consolidée de l’équipe ou du service",
                    "en": "Consolidated team or service response",
                    "es": "Respuesta consolidada del equipo o servicio",
                    "pt": "Resposta consolidada da equipa ou serviço",
                    "ar": "إجابة موحدة للفريق أو الإدارة"
                  }
                },
                {
                  "code": 2,
                  "label": {
                    "fr": "Contribution individuelle fondée sur ma propre pratique",
                    "en": "Individual contribution based on my own practice",
                    "es": "Contribución individual basada en mi propia práctica",
                    "pt": "Contribuição individual baseada na minha prática",
                    "ar": "مساهمة فردية تستند إلى ممارستي"
                  }
                }
              ],
              "source": "Rapport méthodologique du 15 novembre 2026, pp. 28–30, QU01",
              "gate": {
                "field": "QU01_Participation",
                "op": "in",
                "values": [
                  1
                ]
              }
            },
            {
              "id": "QU01_Fonction",
              "label": {
                "fr": "Fonction, sans nom personnel",
                "en": "Function, without a personal name",
                "es": "Función, sin nombre personal",
                "pt": "Função, sem nome pessoal",
                "ar": "الوظيفة، دون اسم شخصي"
              },
              "type": "single",
              "options": [
                {
                  "code": 1,
                  "label": {
                    "fr": "Rédaction de discours ou conseil",
                    "en": "Speechwriting or advisory work",
                    "es": "Redacción de discursos o asesoramiento",
                    "pt": "Redação de discursos ou assessoria",
                    "ar": "كتابة الخطابات أو الاستشارة"
                  }
                },
                {
                  "code": 2,
                  "label": {
                    "fr": "Communication",
                    "en": "Communication",
                    "es": "Comunicación",
                    "pt": "Comunicação",
                    "ar": "الاتصال"
                  }
                },
                {
                  "code": 3,
                  "label": {
                    "fr": "Appui technique ou statistique",
                    "en": "Technical or statistical support",
                    "es": "Apoyo técnico o estadístico",
                    "pt": "Apoio técnico ou estatístico",
                    "ar": "الدعم الفني أو الإحصائي"
                  }
                },
                {
                  "code": 4,
                  "label": {
                    "fr": "Coordination d’équipe",
                    "en": "Team coordination",
                    "es": "Coordinación del equipo",
                    "pt": "Coordenação da equipa",
                    "ar": "تنسيق الفريق"
                  }
                },
                {
                  "code": 97,
                  "label": {
                    "fr": "Autre",
                    "en": "Other",
                    "es": "Otra",
                    "pt": "Outra",
                    "ar": "أخرى"
                  }
                }
              ],
              "source": "Rapport méthodologique du 15 novembre 2026, pp. 28–30, QU01",
              "gate": {
                "field": "QU01_Participation",
                "op": "in",
                "values": [
                  1
                ]
              }
            },
            {
              "id": "QU01_Fonction_Autre",
              "label": {
                "fr": "Autre : précisez",
                "en": "Other: please specify",
                "es": "Otra : especifique",
                "pt": "Outra : especifique",
                "ar": "أخرى : يرجى التحديد"
              },
              "type": "text",
              "options": [],
              "max_chars": 500,
              "gate": {
                "all": [
                  {
                    "field": "QU01_Participation",
                    "op": "in",
                    "values": [
                      1
                    ]
                  },
                  {
                    "field": "QU01_Fonction",
                    "op": "in",
                    "values": [
                      97
                    ]
                  }
                ]
              },
              "source": "Rapport méthodologique du 15 novembre 2026, pp. 28–30, QU01"
            },
            {
              "id": "QU01_Reference_precedente",
              "label": {
                "fr": "Si cet envoi complète une réponse antérieure, indiquer sa référence ou sa date",
                "en": "If this submission supplements an earlier response, give its reference or date",
                "es": "Si este envío complementa una respuesta anterior, indique su referencia o fecha",
                "pt": "Se este envio complementa uma resposta anterior, indique a referência ou data",
                "ar": "إذا كان هذا الإرسال يستكمل إجابة سابقة، فاذكر مرجعها أو تاريخها"
              },
              "type": "text",
              "options": [],
              "max_chars": 500,
              "source": "Rapport méthodologique du 15 novembre 2026, pp. 28–30, QU01",
              "gate": {
                "field": "QU01_Participation",
                "op": "in",
                "values": [
                  1
                ]
              }
            },
            {
              "id": "QU01_Unite",
              "label": {
                "fr": "Service ou unité concernée",
                "en": "Service or unit concerned",
                "es": "Servicio o unidad correspondiente",
                "pt": "Serviço ou unidade em causa",
                "ar": "الإدارة أو الوحدة المعنية"
              },
              "type": "text",
              "options": [],
              "max_chars": 500,
              "source_extra": "Gestion de collecte — § 5.1 ; pas une question substantielle supplémentaire",
              "source": "Gestion de collecte — § 5.1 ; pas une question substantielle supplémentaire",
              "gate": {
                "field": "QU01_Participation",
                "op": "in",
                "values": [
                  1
                ]
              }
            },
            {
              "id": "QU01_Domaine",
              "label": {
                "fr": "Domaine ou secteur, facultatif",
                "en": "Domain or sector, optional",
                "es": "Ámbito o sector, opcional",
                "pt": "Domínio ou setor, facultativo",
                "ar": "المجال أو القطاع، اختياري"
              },
              "type": "text",
              "options": [],
              "max_chars": 500,
              "source_extra": "Gestion de collecte — § 5.1 ; pas une question substantielle supplémentaire",
              "source": "Gestion de collecte — § 5.1 ; pas une question substantielle supplémentaire",
              "gate": {
                "field": "QU01_Participation",
                "op": "in",
                "values": [
                  1
                ]
              }
            },
            {
              "id": "QU01_Langue_texte",
              "label": {
                "fr": "Langue principale de vos réponses libres",
                "en": "Main language of your free-text responses",
                "es": "Idioma principal de sus respuestas abiertas",
                "pt": "Língua principal das suas respostas abertas",
                "ar": "اللغة الأساسية لإجاباتك الحرة"
              },
              "type": "single",
              "options": [
                {
                  "code": "fr",
                  "label": {
                    "fr": "Français",
                    "en": "French",
                    "es": "Francés",
                    "pt": "Francês",
                    "ar": "الفرنسية"
                  }
                },
                {
                  "code": "en",
                  "label": {
                    "fr": "Anglais",
                    "en": "English",
                    "es": "Inglés",
                    "pt": "Inglês",
                    "ar": "الإنجليزية"
                  }
                },
                {
                  "code": "ar",
                  "label": {
                    "fr": "Arabe",
                    "en": "Arabic",
                    "es": "Árabe",
                    "pt": "Árabe",
                    "ar": "العربية"
                  }
                },
                {
                  "code": "pt",
                  "label": {
                    "fr": "Portugais",
                    "en": "Portuguese",
                    "es": "Portugués",
                    "pt": "Português",
                    "ar": "البرتغالية"
                  }
                },
                {
                  "code": "es",
                  "label": {
                    "fr": "Espagnol",
                    "en": "Spanish",
                    "es": "Español",
                    "pt": "Espanhol",
                    "ar": "الإسبانية"
                  }
                }
              ],
              "source_extra": "Gestion de collecte — § 5.1 ; pas une question substantielle supplémentaire",
              "source": "Gestion de collecte — § 5.1 ; pas une question substantielle supplémentaire",
              "gate": {
                "field": "QU01_Participation",
                "op": "in",
                "values": [
                  1
                ]
              }
            },
            {
              "id": "QU01_Origine_invitation",
              "label": {
                "fr": "Origine de l’invitation ou du lien",
                "en": "Origin of the invitation or link",
                "es": "Origen de la invitación o del enlace",
                "pt": "Origem do convite ou da ligação",
                "ar": "مصدر الدعوة أو الرابط"
              },
              "type": "single",
              "options": [
                {
                  "code": 1,
                  "label": {
                    "fr": "INS",
                    "en": "NSO",
                    "es": "ONE",
                    "pt": "INE",
                    "ar": "الجهاز الوطني للإحصاء"
                  }
                },
                {
                  "code": 2,
                  "label": {
                    "fr": "STATAFRIC ou consultant",
                    "en": "STATAFRIC or consultant",
                    "es": "STATAFRIC o consultor",
                    "pt": "STATAFRIC ou consultor",
                    "ar": "STATAFRIC أو الاستشاري"
                  }
                },
                {
                  "code": 3,
                  "label": {
                    "fr": "Autre relais",
                    "en": "Other forwarding organisation",
                    "es": "Otra entidad que transmitió la invitación",
                    "pt": "Outra entidade que encaminhou o convite",
                    "ar": "جهة أخرى أحالت الدعوة"
                  }
                },
                {
                  "code": 4,
                  "label": {
                    "fr": "Accès spontané",
                    "en": "Unsolicited access",
                    "es": "Acceso espontáneo",
                    "pt": "Acesso espontâneo",
                    "ar": "وصول تلقائي دون دعوة مباشرة"
                  }
                },
                {
                  "code": 98,
                  "label": {
                    "fr": "Ne sait pas",
                    "en": "Do not know",
                    "es": "No sabe",
                    "pt": "Não sabe",
                    "ar": "لا أعرف"
                  }
                }
              ],
              "source_extra": "Gestion de collecte — § 5.1 ; pas une question substantielle supplémentaire",
              "source": "Gestion de collecte — § 5.1 ; pas une question substantielle supplémentaire",
              "gate": {
                "field": "QU01_Participation",
                "op": "in",
                "values": [
                  1
                ]
              }
            },
            {
              "id": "QU01_INS_relais",
              "label": {
                "fr": "INS ou organisme relais, facultatif",
                "en": "Forwarding NSO or organisation, optional",
                "es": "ONE o entidad que transmitió la invitación, opcional",
                "pt": "INE ou entidade que encaminhou o convite, facultativo",
                "ar": "الجهاز الوطني للإحصاء أو الجهة المحيلة، اختياري"
              },
              "type": "text",
              "options": [],
              "max_chars": 500,
              "source_extra": "Gestion de collecte — § 5.1 ; pas une question substantielle supplémentaire",
              "source": "Gestion de collecte — § 5.1 ; pas une question substantielle supplémentaire",
              "gate": {
                "field": "QU01_Participation",
                "op": "in",
                "values": [
                  1
                ]
              }
            }
          ],
          "note": {
            "fr": "Répondre pour une équipe ou un service préparant des discours, ou sur votre pratique personnelle si vous n’êtes pas mandaté pour l’équipe. Les informations portent sur les chiffres, sans évaluation politique des discours.",
            "en": "Respond for a speech-preparation team or service, or describe your own practice if you are not mandated to respond for the team. Information concerns figures, without any political assessment of speeches.",
            "es": "Responda por un equipo o servicio que prepara discursos, o describa su práctica si no tiene mandato para responder en su nombre. La información se refiere a las cifras, sin evaluación política de los discursos.",
            "pt": "Responda por uma equipa ou serviço que prepara discursos, ou descreva a sua prática se não tiver mandato para responder em seu nome. A informação refere-se aos números, sem avaliação política dos discursos.",
            "ar": "أجب باسم فريق أو إدارة تعد الخطابات، أو صف ممارستك إذا لم تكن مفوضا للإجابة باسم الفريق. تتعلق المعلومات بالأرقام، دون تقييم سياسي للخطابات."
          },
          "gate": null,
          "theme": "Suivi et moyens",
          "source": "Rapport méthodologique du 15 novembre 2026, pp. 28–30, QU01"
        },
        {
          "id": "QU02",
          "title": {
            "fr": "Activité pendant les douze mois de référence",
            "en": "Activity during the twelve-month reference period",
            "es": "Actividad durante los doce meses de referencia",
            "pt": "Atividade nos doze meses de referência",
            "ar": "النشاط خلال الاثني عشر شهرا المرجعية"
          },
          "fields": [
            {
              "id": "QU02a",
              "label": {
                "fr": "À combien de discours comportant des chiffres votre équipe ou vous-même avez-vous contribué ?",
                "en": "How many speeches containing figures did your team or you contribute to?",
                "es": "¿En cuántos discursos con cifras participó su equipo o usted?",
                "pt": "Em quantos discursos com números participou a sua equipa ou você?",
                "ar": "كم خطابا يتضمن أرقاما أسهمتم أو أسهم فريقكم في إعداده؟"
              },
              "type": "single",
              "options": [
                {
                  "code": 0,
                  "label": {
                    "fr": "Aucune / aucun",
                    "en": "None",
                    "es": "Ninguna",
                    "pt": "Nenhum",
                    "ar": "لا يوجد"
                  }
                },
                {
                  "code": 1,
                  "label": {
                    "fr": "De 1 à 5",
                    "en": "1 to 5",
                    "es": "De 1 a 5",
                    "pt": "De 1 a 5",
                    "ar": "من 1 إلى 5"
                  }
                },
                {
                  "code": 2,
                  "label": {
                    "fr": "De 6 à 20",
                    "en": "6 to 20",
                    "es": "De 6 a 20",
                    "pt": "De 6 a 20",
                    "ar": "من 6 إلى 20"
                  }
                },
                {
                  "code": 3,
                  "label": {
                    "fr": "De 21 à 50",
                    "en": "21 to 50",
                    "es": "De 21 a 50",
                    "pt": "De 21 a 50",
                    "ar": "من 21 إلى 50"
                  }
                },
                {
                  "code": 4,
                  "label": {
                    "fr": "Plus de 50",
                    "en": "More than 50",
                    "es": "Más de 50",
                    "pt": "Mais de 50",
                    "ar": "أكثر من 50"
                  }
                },
                {
                  "code": 98,
                  "label": {
                    "fr": "Ne sait pas",
                    "en": "Do not know",
                    "es": "No sabe",
                    "pt": "Não sabe",
                    "ar": "لا أعرف"
                  }
                }
              ],
              "source": "Rapport méthodologique du 15 novembre 2026, pp. 28–30, QU02",
              "gate": {
                "field": "QU01_Participation",
                "op": "in",
                "values": [
                  1
                ]
              }
            },
            {
              "id": "QU02b",
              "label": {
                "fr": "Avez-vous demandé un appui statistique à l’INS pour au moins un de ces discours ?",
                "en": "Did you request statistical support from the NSO for at least one of these speeches?",
                "es": "¿Solicitó apoyo estadístico a la ONE para al menos uno de esos discursos?",
                "pt": "Solicitou apoio estatístico ao INE para pelo menos um desses discursos?",
                "ar": "هل طلبتم دعما إحصائيا من الجهاز الوطني للإحصاء لخطاب واحد على الأقل من هذه الخطابات؟"
              },
              "type": "single",
              "options": [
                {
                  "code": 1,
                  "label": {
                    "fr": "Oui",
                    "en": "Yes",
                    "es": "Sí",
                    "pt": "Sim",
                    "ar": "نعم"
                  }
                },
                {
                  "code": 0,
                  "label": {
                    "fr": "Non",
                    "en": "No",
                    "es": "No",
                    "pt": "Não",
                    "ar": "لا"
                  }
                },
                {
                  "code": 98,
                  "label": {
                    "fr": "Ne sait pas",
                    "en": "Do not know",
                    "es": "No sabe",
                    "pt": "Não sabe",
                    "ar": "لا أعرف"
                  }
                },
                {
                  "code": 99,
                  "label": {
                    "fr": "Non applicable, aucun discours concerné",
                    "en": "Not applicable, no relevant speech",
                    "es": "No aplicable, ningún discurso pertinente",
                    "pt": "Não aplicável, nenhum discurso em causa",
                    "ar": "لا ينطبق، لا يوجد خطاب معني"
                  }
                }
              ],
              "source": "Rapport méthodologique du 15 novembre 2026, pp. 28–30, QU02",
              "gate": {
                "field": "QU01_Participation",
                "op": "in",
                "values": [
                  1
                ]
              }
            },
            {
              "id": "QU02_Base",
              "label": {
                "fr": "Base de la réponse",
                "en": "Basis for the response",
                "es": "Base de la respuesta",
                "pt": "Base da resposta",
                "ar": "أساس الإجابة"
              },
              "type": "single",
              "options": [
                {
                  "code": 1,
                  "label": {
                    "fr": "Registre ou inventaire consulté",
                    "en": "Register or inventory consulted",
                    "es": "Registro o inventario consultado",
                    "pt": "Registo ou inventário consultado",
                    "ar": "سجل أو حصر تمت مراجعته"
                  }
                },
                {
                  "code": 2,
                  "label": {
                    "fr": "Estimation fondée sur la pratique connue",
                    "en": "Estimate based on known practice",
                    "es": "Estimación basada en la práctica conocida",
                    "pt": "Estimativa baseada na prática conhecida",
                    "ar": "تقدير يستند إلى الممارسة المعروفة"
                  }
                },
                {
                  "code": 98,
                  "label": {
                    "fr": "Ne sait pas",
                    "en": "Do not know",
                    "es": "No sabe",
                    "pt": "Não sabe",
                    "ar": "لا أعرف"
                  }
                }
              ],
              "source": "Rapport méthodologique du 15 novembre 2026, pp. 28–30, QU02",
              "gate": {
                "field": "QU01_Participation",
                "op": "in",
                "values": [
                  1
                ]
              }
            }
          ],
          "note": {
            "fr": "Compter une intervention une seule fois, même si elle a fait l’objet de plusieurs versions. Si aucun discours n’est concerné, passer à QU07. Si le nombre est inconnu, poursuivre lorsqu’un cas réel est connu.",
            "en": "Count an intervention only once, even if several versions were prepared. If there were no relevant speeches, proceed to QU07. If the number is unknown, continue when a real case is known.",
            "es": "Cuente cada intervención una sola vez, aunque tenga varias versiones. Si no hubo discursos pertinentes, pase a QU07. Si desconoce el número, continúe cuando conozca un caso real.",
            "pt": "Conte cada intervenção uma só vez, mesmo com várias versões. Se não houve discursos em causa, avance para QU07. Se desconhece o número, continue quando conhece um caso real.",
            "ar": "احسب المداخلة مرة واحدة، حتى إن أعدت لها عدة نسخ. إذا لم توجد خطابات معنية، انتقل إلى QU07. وإذا جهل العدد، تابع عند معرفة حالة فعلية."
          },
          "gate": null,
          "theme": "Demande et urgence",
          "source": "Rapport méthodologique du 15 novembre 2026, pp. 28–30, QU02"
        },
        {
          "id": "QU03",
          "title": {
            "fr": "Dernier discours préparé comportant des chiffres",
            "en": "Most recent speech prepared containing figures",
            "es": "Último discurso preparado que incluía cifras",
            "pt": "Último discurso preparado com números",
            "ar": "أحدث خطاب أعد ويتضمن أرقاما"
          },
          "fields": [
            {
              "id": "QU03_Cas",
              "label": {
                "fr": "Un tel cas peut-il être décrit ?",
                "en": "Can such a case be described?",
                "es": "¿Puede describir un caso de este tipo?",
                "pt": "Pode descrever um caso deste tipo?",
                "ar": "هل يمكن وصف حالة من هذا النوع؟"
              },
              "type": "single",
              "options": [
                {
                  "code": 1,
                  "label": {
                    "fr": "Oui",
                    "en": "Yes",
                    "es": "Sí",
                    "pt": "Sim",
                    "ar": "نعم"
                  }
                },
                {
                  "code": 0,
                  "label": {
                    "fr": "Non",
                    "en": "No",
                    "es": "No",
                    "pt": "Não",
                    "ar": "لا"
                  }
                },
                {
                  "code": 98,
                  "label": {
                    "fr": "Ne sait pas",
                    "en": "Do not know",
                    "es": "No sabe",
                    "pt": "Não sabe",
                    "ar": "لا أعرف"
                  }
                }
              ],
              "source": "Rapport méthodologique du 15 novembre 2026, pp. 28–30, QU03",
              "gate": {
                "field": "QU01_Participation",
                "op": "in",
                "values": [
                  1
                ]
              }
            },
            {
              "id": "QU03_Mois",
              "label": {
                "fr": "Mois et année de fin de préparation, MM/AAAA",
                "en": "Month and year preparation ended, MM/YYYY",
                "es": "Mes y año de finalización de la preparación, MM/AAAA",
                "pt": "Mês e ano do fim da preparação, MM/AAAA",
                "ar": "شهر وسنة انتهاء الإعداد، MM/YYYY"
              },
              "type": "month",
              "options": [],
              "max_chars": 7,
              "gate": {
                "all": [
                  {
                    "field": "QU01_Participation",
                    "op": "in",
                    "values": [
                      1
                    ]
                  },
                  {
                    "field": "QU03_Cas",
                    "op": "in",
                    "values": [
                      1
                    ]
                  }
                ]
              },
              "source": "Rapport méthodologique du 15 novembre 2026, pp. 28–30, QU03"
            },
            {
              "id": "QU03_Theme",
              "label": {
                "fr": "Thème général, sans nom de personne",
                "en": "General topic, without a person’s name",
                "es": "Tema general, sin nombre de persona",
                "pt": "Tema geral, sem nome de pessoa",
                "ar": "الموضوع العام، دون اسم شخص"
              },
              "type": "text",
              "options": [],
              "max_chars": 500,
              "gate": {
                "all": [
                  {
                    "field": "QU01_Participation",
                    "op": "in",
                    "values": [
                      1
                    ]
                  },
                  {
                    "field": "QU03_Cas",
                    "op": "in",
                    "values": [
                      1
                    ]
                  }
                ]
              },
              "source": "Rapport méthodologique du 15 novembre 2026, pp. 28–30, QU03"
            },
            {
              "id": "QU03_Public",
              "label": {
                "fr": "Public principal",
                "en": "Main audience",
                "es": "Público principal",
                "pt": "Público principal",
                "ar": "الجمهور الرئيسي"
              },
              "type": "single",
              "options": [
                {
                  "code": 1,
                  "label": {
                    "fr": "Grand public",
                    "en": "General public",
                    "es": "Público general",
                    "pt": "Público em geral",
                    "ar": "الجمهور العام"
                  }
                },
                {
                  "code": 2,
                  "label": {
                    "fr": "Parlement ou élus",
                    "en": "Parliament or elected representatives",
                    "es": "Parlamento o cargos electos",
                    "pt": "Parlamento ou eleitos",
                    "ar": "البرلمان أو المنتخبون"
                  }
                },
                {
                  "code": 3,
                  "label": {
                    "fr": "Public technique ou professionnel",
                    "en": "Technical or professional audience",
                    "es": "Público técnico o profesional",
                    "pt": "Público técnico ou profissional",
                    "ar": "جمهور فني أو مهني"
                  }
                },
                {
                  "code": 4,
                  "label": {
                    "fr": "Communauté locale",
                    "en": "Local community",
                    "es": "Comunidad local",
                    "pt": "Comunidade local",
                    "ar": "مجتمع محلي"
                  }
                },
                {
                  "code": 97,
                  "label": {
                    "fr": "Autre",
                    "en": "Other",
                    "es": "Otra",
                    "pt": "Outra",
                    "ar": "أخرى"
                  }
                },
                {
                  "code": 98,
                  "label": {
                    "fr": "Ne sait pas",
                    "en": "Do not know",
                    "es": "No sabe",
                    "pt": "Não sabe",
                    "ar": "لا أعرف"
                  }
                }
              ],
              "gate": {
                "all": [
                  {
                    "field": "QU01_Participation",
                    "op": "in",
                    "values": [
                      1
                    ]
                  },
                  {
                    "field": "QU03_Cas",
                    "op": "in",
                    "values": [
                      1
                    ]
                  }
                ]
              },
              "source": "Rapport méthodologique du 15 novembre 2026, pp. 28–30, QU03"
            },
            {
              "id": "QU03_Public_Autre",
              "label": {
                "fr": "Autre : précisez",
                "en": "Other: please specify",
                "es": "Otra : especifique",
                "pt": "Outra : especifique",
                "ar": "أخرى : يرجى التحديد"
              },
              "type": "text",
              "options": [],
              "max_chars": 500,
              "gate": {
                "all": [
                  {
                    "field": "QU01_Participation",
                    "op": "in",
                    "values": [
                      1
                    ]
                  },
                  {
                    "field": "QU03_Public",
                    "op": "in",
                    "values": [
                      97
                    ]
                  }
                ]
              },
              "source": "Rapport méthodologique du 15 novembre 2026, pp. 28–30, QU03"
            },
            {
              "id": "QU03_Langues",
              "label": {
                "fr": "Langue ou langues du discours",
                "en": "Language(s) of the speech",
                "es": "Idioma o idiomas del discurso",
                "pt": "Língua ou línguas do discurso",
                "ar": "لغة الخطاب أو لغاته"
              },
              "type": "text",
              "options": [],
              "max_chars": 500,
              "gate": {
                "all": [
                  {
                    "field": "QU01_Participation",
                    "op": "in",
                    "values": [
                      1
                    ]
                  },
                  {
                    "field": "QU03_Cas",
                    "op": "in",
                    "values": [
                      1
                    ]
                  }
                ]
              },
              "source": "Rapport méthodologique du 15 novembre 2026, pp. 28–30, QU03"
            },
            {
              "id": "QU03_Origine",
              "label": {
                "fr": "Origine du premier chiffre utilisé",
                "en": "Origin of the first figure used",
                "es": "Origen de la primera cifra utilizada",
                "pt": "Origem do primeiro número utilizado",
                "ar": "مصدر أول رقم مستخدم"
              },
              "type": "single",
              "options": [
                {
                  "code": 1,
                  "label": {
                    "fr": "Source publiée de l’INS consultée directement",
                    "en": "Published NSO source consulted directly",
                    "es": "Fuente publicada por la ONE consultada directamente",
                    "pt": "Fonte publicada pelo INE consultada diretamente",
                    "ar": "مصدر منشور للجهاز الوطني للإحصاء تمت مراجعته مباشرة"
                  }
                },
                {
                  "code": 2,
                  "label": {
                    "fr": "Chiffre transmis directement par l’INS",
                    "en": "Figure supplied directly by the NSO",
                    "es": "Cifra transmitida directamente por la ONE",
                    "pt": "Número transmitido diretamente pelo INE",
                    "ar": "رقم قدمه الجهاز الوطني للإحصاء مباشرة"
                  }
                },
                {
                  "code": 3,
                  "label": {
                    "fr": "Autre administration nationale",
                    "en": "Other national administration",
                    "es": "Otra administración nacional",
                    "pt": "Outra administração nacional",
                    "ar": "إدارة وطنية أخرى"
                  }
                },
                {
                  "code": 4,
                  "label": {
                    "fr": "Organisation régionale ou internationale",
                    "en": "Regional or international organisation",
                    "es": "Organización regional o internacional",
                    "pt": "Organização regional ou internacional",
                    "ar": "منظمة إقليمية أو دولية"
                  }
                },
                {
                  "code": 5,
                  "label": {
                    "fr": "Recherche ou source privée identifiée",
                    "en": "Identified research or private source",
                    "es": "Investigación o fuente privada identificada",
                    "pt": "Investigação ou fonte privada identificada",
                    "ar": "بحث أو مصدر خاص محدد"
                  }
                },
                {
                  "code": 6,
                  "label": {
                    "fr": "Discours ou document précédent sans retour à la source statistique",
                    "en": "Previous speech or document, without consulting the statistical source",
                    "es": "Discurso o documento previo sin consultar la fuente estadística",
                    "pt": "Discurso ou documento anterior sem consultar a fonte estatística",
                    "ar": "خطاب أو وثيقة سابقة دون الرجوع إلى المصدر الإحصائي"
                  }
                },
                {
                  "code": 7,
                  "label": {
                    "fr": "Source non identifiée",
                    "en": "Unidentified source",
                    "es": "Fuente no identificada",
                    "pt": "Fonte não identificada",
                    "ar": "مصدر غير محدد"
                  }
                },
                {
                  "code": 97,
                  "label": {
                    "fr": "Autre",
                    "en": "Other",
                    "es": "Otra",
                    "pt": "Outra",
                    "ar": "أخرى"
                  }
                },
                {
                  "code": 98,
                  "label": {
                    "fr": "Ne sait pas",
                    "en": "Do not know",
                    "es": "No sabe",
                    "pt": "Não sabe",
                    "ar": "لا أعرف"
                  }
                }
              ],
              "gate": {
                "all": [
                  {
                    "field": "QU01_Participation",
                    "op": "in",
                    "values": [
                      1
                    ]
                  },
                  {
                    "field": "QU03_Cas",
                    "op": "in",
                    "values": [
                      1
                    ]
                  }
                ]
              },
              "source": "Rapport méthodologique du 15 novembre 2026, pp. 28–30, QU03"
            },
            {
              "id": "QU03_Origine_Autre",
              "label": {
                "fr": "Autre : précisez",
                "en": "Other: please specify",
                "es": "Otra : especifique",
                "pt": "Outra : especifique",
                "ar": "أخرى : يرجى التحديد"
              },
              "type": "text",
              "options": [],
              "max_chars": 500,
              "gate": {
                "all": [
                  {
                    "field": "QU01_Participation",
                    "op": "in",
                    "values": [
                      1
                    ]
                  },
                  {
                    "field": "QU03_Origine",
                    "op": "in",
                    "values": [
                      97
                    ]
                  }
                ]
              },
              "source": "Rapport méthodologique du 15 novembre 2026, pp. 28–30, QU03"
            },
            {
              "id": "QU03_Delai",
              "label": {
                "fr": "Si un appui a été demandé à l’INS pour ce cas, délai demandé",
                "en": "If NSO support was requested for this case, requested turnaround time",
                "es": "Si se solicitó apoyo a la ONE para este caso, plazo solicitado",
                "pt": "Se foi solicitado apoio ao INE para este caso, prazo solicitado",
                "ar": "إذا طلب دعم الجهاز الوطني للإحصاء لهذه الحالة، فما المهلة المطلوبة؟"
              },
              "type": "single",
              "options": [
                {
                  "code": 1,
                  "label": {
                    "fr": "Moins de 6 heures",
                    "en": "Less than 6 hours",
                    "es": "Menos de 6 horas",
                    "pt": "Menos de 6 horas",
                    "ar": "أقل من 6 ساعات"
                  }
                },
                {
                  "code": 2,
                  "label": {
                    "fr": "De 6 à 24 heures incluses",
                    "en": "6 to 24 hours inclusive",
                    "es": "De 6 a 24 horas, inclusive",
                    "pt": "De 6 a 24 horas, inclusive",
                    "ar": "من 6 إلى 24 ساعة شاملة"
                  }
                },
                {
                  "code": 3,
                  "label": {
                    "fr": "Plus de 24 heures à 3 jours inclus",
                    "en": "More than 24 hours up to 3 days inclusive",
                    "es": "Más de 24 horas y hasta 3 días, inclusive",
                    "pt": "Mais de 24 horas até 3 dias, inclusive",
                    "ar": "أكثر من 24 ساعة وحتى 3 أيام شاملة"
                  }
                },
                {
                  "code": 4,
                  "label": {
                    "fr": "Plus de 3 jours",
                    "en": "More than 3 days",
                    "es": "Más de 3 días",
                    "pt": "Mais de 3 dias",
                    "ar": "أكثر من 3 أيام"
                  }
                },
                {
                  "code": 5,
                  "label": {
                    "fr": "Aucune échéance indiquée",
                    "en": "No deadline specified",
                    "es": "Sin plazo especificado",
                    "pt": "Sem prazo indicado",
                    "ar": "لم تحدد مهلة"
                  }
                },
                {
                  "code": 98,
                  "label": {
                    "fr": "Ne sait pas",
                    "en": "Do not know",
                    "es": "No sabe",
                    "pt": "Não sabe",
                    "ar": "لا أعرف"
                  }
                },
                {
                  "code": 99,
                  "label": {
                    "fr": "Aucun appui demandé à l’INS pour ce cas",
                    "en": "No NSO support requested for this case",
                    "es": "No se solicitó apoyo a la ONE para este caso",
                    "pt": "Não foi solicitado apoio ao INE para este caso",
                    "ar": "لم يطلب دعم الجهاز الوطني للإحصاء لهذه الحالة"
                  }
                }
              ],
              "gate": {
                "all": [
                  {
                    "field": "QU01_Participation",
                    "op": "in",
                    "values": [
                      1
                    ]
                  },
                  {
                    "field": "QU03_Cas",
                    "op": "in",
                    "values": [
                      1
                    ]
                  }
                ]
              },
              "source": "Rapport méthodologique du 15 novembre 2026, pp. 28–30, QU03"
            },
            {
              "id": "QU03_Issue",
              "label": {
                "fr": "Si un appui a été demandé, son issue",
                "en": "If support was requested, its outcome",
                "es": "Si se solicitó apoyo, resultado obtenido",
                "pt": "Se foi solicitado apoio, resultado obtido",
                "ar": "إذا طلب دعم، فما النتيجة؟"
              },
              "type": "single",
              "options": [
                {
                  "code": 1,
                  "label": {
                    "fr": "Chiffres reçus à temps",
                    "en": "Figures received on time",
                    "es": "Cifras recibidas a tiempo",
                    "pt": "Números recebidos a tempo",
                    "ar": "استلمت الأرقام في الوقت المناسب"
                  }
                },
                {
                  "code": 2,
                  "label": {
                    "fr": "Chiffres reçus après l’échéance",
                    "en": "Figures received after the deadline",
                    "es": "Cifras recibidas después del plazo",
                    "pt": "Números recebidos após o prazo",
                    "ar": "استلمت الأرقام بعد المهلة"
                  }
                },
                {
                  "code": 3,
                  "label": {
                    "fr": "Aucun chiffre reçu",
                    "en": "No figures received",
                    "es": "No se recibieron cifras",
                    "pt": "Não foram recebidos números",
                    "ar": "لم تستلم أرقام"
                  }
                },
                {
                  "code": 4,
                  "label": {
                    "fr": "Chiffres reçus, sans échéance convenue",
                    "en": "Figures received, with no agreed deadline",
                    "es": "Cifras recibidas sin plazo convenido",
                    "pt": "Números recebidos sem prazo acordado",
                    "ar": "استلمت الأرقام دون مهلة متفق عليها"
                  }
                },
                {
                  "code": 98,
                  "label": {
                    "fr": "Ne sait pas",
                    "en": "Do not know",
                    "es": "No sabe",
                    "pt": "Não sabe",
                    "ar": "لا أعرف"
                  }
                },
                {
                  "code": 99,
                  "label": {
                    "fr": "Aucun appui demandé",
                    "en": "No support requested",
                    "es": "No se solicitó apoyo",
                    "pt": "Não foi solicitado apoio",
                    "ar": "لم يطلب دعم"
                  }
                }
              ],
              "gate": {
                "all": [
                  {
                    "field": "QU01_Participation",
                    "op": "in",
                    "values": [
                      1
                    ]
                  },
                  {
                    "field": "QU03_Cas",
                    "op": "in",
                    "values": [
                      1
                    ]
                  }
                ]
              },
              "source": "Rapport méthodologique du 15 novembre 2026, pp. 28–30, QU03"
            },
            {
              "id": "QU03_Exemple",
              "label": {
                "fr": "Cas partageable, facultatif : lien vers le discours public ou bref extrait non confidentiel autorisé, avec sa date",
                "en": "Shareable case, optional: link to the public speech or brief authorised non-confidential extract, with its date",
                "es": "Caso compartible, opcional : enlace al discurso público o breve extracto autorizado no confidencial, con fecha",
                "pt": "Caso partilhável, facultativo : ligação ao discurso público ou breve excerto autorizado não confidencial, com data",
                "ar": "حالة قابلة للمشاركة، اختياري : رابط الخطاب العام أو مقتطف موجز غير سري ومصرح به، مع التاريخ"
              },
              "type": "textarea",
              "options": [],
              "max_chars": 2500,
              "gate": {
                "all": [
                  {
                    "field": "QU01_Participation",
                    "op": "in",
                    "values": [
                      1
                    ]
                  },
                  {
                    "field": "QU03_Cas",
                    "op": "in",
                    "values": [
                      1
                    ]
                  }
                ]
              },
              "source": "Rapport méthodologique du 15 novembre 2026, pp. 28–30, QU03"
            }
          ],
          "note": {
            "fr": "Considérer le discours comportant des chiffres dont la préparation s’est achevée le plus récemment pendant la période, et non un exemple sélectionné pour sa qualité. Si aucun cas n’est décrit, passer à QU07. L’absence de lien ne remet pas en cause votre réponse.",
            "en": "Consider the speech containing figures whose preparation ended most recently during the period, rather than an example chosen for its quality. If no case can be described, proceed to QU07. The absence of a link does not invalidate your response.",
            "es": "Considere el discurso con cifras cuya preparación terminó más recientemente durante el período, no un caso elegido por su calidad. Si no puede describir un caso, pase a QU07. La falta de un enlace no invalida su respuesta.",
            "pt": "Considere o discurso com números cuja preparação terminou mais recentemente no período, não um caso escolhido pela qualidade. Se não puder descrever um caso, avance para QU07. A falta de ligação não invalida a resposta.",
            "ar": "اعتمد الخطاب المتضمن أرقاما الذي انتهى إعداده مؤخرا خلال الفترة، لا مثالا اختير لجودته. إذا تعذر وصف حالة، انتقل إلى QU07. لا يبطل غياب الرابط إجابتك."
          },
          "gate": {
            "field": "QU02a",
            "op": "not_in",
            "values": [
              0
            ]
          },
          "theme": "Messages et publics",
          "source": "Rapport méthodologique du 15 novembre 2026, pp. 28–30, QU03"
        },
        {
          "id": "QU04",
          "title": {
            "fr": "Informations disponibles pour comprendre ce chiffre",
            "en": "Information available to understand this figure",
            "es": "Información disponible para comprender esta cifra",
            "pt": "Informações disponíveis para compreender este número",
            "ar": "المعلومات المتاحة لفهم هذا الرقم"
          },
          "fields": [
            {
              "id": "QU04_M1",
              "label": {
                "fr": "Source permettant de retrouver le résultat",
                "en": "Source enabling the result to be located",
                "es": "Fuente que permite localizar el resultado",
                "pt": "Fonte que permite localizar o resultado",
                "ar": "مصدر يتيح العثور على النتيجة"
              },
              "type": "single",
              "options": [
                {
                  "code": 1,
                  "label": {
                    "fr": "Dans le document ou message utilisé",
                    "en": "In the document or message used",
                    "es": "En el documento o mensaje utilizado",
                    "pt": "No documento ou mensagem utilizados",
                    "ar": "في الوثيقة أو الرسالة المستخدمة"
                  }
                },
                {
                  "code": 2,
                  "label": {
                    "fr": "Accessible par un lien ou une référence précise disponible",
                    "en": "Accessible through an available specific link or reference",
                    "es": "Accesible mediante un enlace o referencia precisa disponible",
                    "pt": "Acessível por uma ligação ou referência precisa disponível",
                    "ar": "متاحة عبر رابط أو مرجع دقيق"
                  }
                },
                {
                  "code": 0,
                  "label": {
                    "fr": "Absente",
                    "en": "Absent",
                    "es": "Ausente",
                    "pt": "Ausente",
                    "ar": "غير موجودة"
                  }
                },
                {
                  "code": 98,
                  "label": {
                    "fr": "Ne sait pas",
                    "en": "Do not know",
                    "es": "No sabe",
                    "pt": "Não sabe",
                    "ar": "لا أعرف"
                  }
                },
                {
                  "code": 99,
                  "label": {
                    "fr": "Non applicable",
                    "en": "Not applicable",
                    "es": "No aplicable",
                    "pt": "Não aplicável",
                    "ar": "لا ينطبق"
                  }
                }
              ],
              "source": "Rapport méthodologique du 15 novembre 2026, pp. 28–30, QU04",
              "gate": {
                "field": "QU01_Participation",
                "op": "in",
                "values": [
                  1
                ]
              }
            },
            {
              "id": "QU04_M2",
              "label": {
                "fr": "Date de publication ou version",
                "en": "Publication date or version",
                "es": "Fecha de publicación o versión",
                "pt": "Data de publicação ou versão",
                "ar": "تاريخ النشر أو النسخة"
              },
              "type": "single",
              "options": [
                {
                  "code": 1,
                  "label": {
                    "fr": "Dans le document ou message utilisé",
                    "en": "In the document or message used",
                    "es": "En el documento o mensaje utilizado",
                    "pt": "No documento ou mensagem utilizados",
                    "ar": "في الوثيقة أو الرسالة المستخدمة"
                  }
                },
                {
                  "code": 2,
                  "label": {
                    "fr": "Accessible par un lien ou une référence précise disponible",
                    "en": "Accessible through an available specific link or reference",
                    "es": "Accesible mediante un enlace o referencia precisa disponible",
                    "pt": "Acessível por uma ligação ou referência precisa disponível",
                    "ar": "متاحة عبر رابط أو مرجع دقيق"
                  }
                },
                {
                  "code": 0,
                  "label": {
                    "fr": "Absente",
                    "en": "Absent",
                    "es": "Ausente",
                    "pt": "Ausente",
                    "ar": "غير موجودة"
                  }
                },
                {
                  "code": 98,
                  "label": {
                    "fr": "Ne sait pas",
                    "en": "Do not know",
                    "es": "No sabe",
                    "pt": "Não sabe",
                    "ar": "لا أعرف"
                  }
                },
                {
                  "code": 99,
                  "label": {
                    "fr": "Non applicable",
                    "en": "Not applicable",
                    "es": "No aplicable",
                    "pt": "Não aplicável",
                    "ar": "لا ينطبق"
                  }
                }
              ],
              "source": "Rapport méthodologique du 15 novembre 2026, pp. 28–30, QU04",
              "gate": {
                "field": "QU01_Participation",
                "op": "in",
                "values": [
                  1
                ]
              }
            },
            {
              "id": "QU04_M3",
              "label": {
                "fr": "Période de la valeur",
                "en": "Period of the value",
                "es": "Período del valor",
                "pt": "Período do valor",
                "ar": "الفترة الخاصة بالقيمة"
              },
              "type": "single",
              "options": [
                {
                  "code": 1,
                  "label": {
                    "fr": "Dans le document ou message utilisé",
                    "en": "In the document or message used",
                    "es": "En el documento o mensaje utilizado",
                    "pt": "No documento ou mensagem utilizados",
                    "ar": "في الوثيقة أو الرسالة المستخدمة"
                  }
                },
                {
                  "code": 2,
                  "label": {
                    "fr": "Accessible par un lien ou une référence précise disponible",
                    "en": "Accessible through an available specific link or reference",
                    "es": "Accesible mediante un enlace o referencia precisa disponible",
                    "pt": "Acessível por uma ligação ou referência precisa disponível",
                    "ar": "متاحة عبر رابط أو مرجع دقيق"
                  }
                },
                {
                  "code": 0,
                  "label": {
                    "fr": "Absente",
                    "en": "Absent",
                    "es": "Ausente",
                    "pt": "Ausente",
                    "ar": "غير موجودة"
                  }
                },
                {
                  "code": 98,
                  "label": {
                    "fr": "Ne sait pas",
                    "en": "Do not know",
                    "es": "No sabe",
                    "pt": "Não sabe",
                    "ar": "لا أعرف"
                  }
                },
                {
                  "code": 99,
                  "label": {
                    "fr": "Non applicable",
                    "en": "Not applicable",
                    "es": "No aplicable",
                    "pt": "Não aplicável",
                    "ar": "لا ينطبق"
                  }
                }
              ],
              "source": "Rapport méthodologique du 15 novembre 2026, pp. 28–30, QU04",
              "gate": {
                "field": "QU01_Participation",
                "op": "in",
                "values": [
                  1
                ]
              }
            },
            {
              "id": "QU04_M4",
              "label": {
                "fr": "Définition de l’indicateur",
                "en": "Definition of the indicator",
                "es": "Definición del indicador",
                "pt": "Definição do indicador",
                "ar": "تعريف المؤشر"
              },
              "type": "single",
              "options": [
                {
                  "code": 1,
                  "label": {
                    "fr": "Dans le document ou message utilisé",
                    "en": "In the document or message used",
                    "es": "En el documento o mensaje utilizado",
                    "pt": "No documento ou mensagem utilizados",
                    "ar": "في الوثيقة أو الرسالة المستخدمة"
                  }
                },
                {
                  "code": 2,
                  "label": {
                    "fr": "Accessible par un lien ou une référence précise disponible",
                    "en": "Accessible through an available specific link or reference",
                    "es": "Accesible mediante un enlace o referencia precisa disponible",
                    "pt": "Acessível por uma ligação ou referência precisa disponível",
                    "ar": "متاحة عبر رابط أو مرجع دقيق"
                  }
                },
                {
                  "code": 0,
                  "label": {
                    "fr": "Absente",
                    "en": "Absent",
                    "es": "Ausente",
                    "pt": "Ausente",
                    "ar": "غير موجودة"
                  }
                },
                {
                  "code": 98,
                  "label": {
                    "fr": "Ne sait pas",
                    "en": "Do not know",
                    "es": "No sabe",
                    "pt": "Não sabe",
                    "ar": "لا أعرف"
                  }
                },
                {
                  "code": 99,
                  "label": {
                    "fr": "Non applicable",
                    "en": "Not applicable",
                    "es": "No aplicable",
                    "pt": "Não aplicável",
                    "ar": "لا ينطبق"
                  }
                }
              ],
              "source": "Rapport méthodologique du 15 novembre 2026, pp. 28–30, QU04",
              "gate": {
                "field": "QU01_Participation",
                "op": "in",
                "values": [
                  1
                ]
              }
            },
            {
              "id": "QU04_M5",
              "label": {
                "fr": "Unité et base de calcul du taux, le cas échéant",
                "en": "Unit and calculation basis of the rate, where applicable",
                "es": "Unidad y base de cálculo de la tasa, si procede",
                "pt": "Unidade e base de cálculo da taxa, se aplicável",
                "ar": "الوحدة وأساس حساب المعدل، عند الاقتضاء"
              },
              "type": "single",
              "options": [
                {
                  "code": 1,
                  "label": {
                    "fr": "Dans le document ou message utilisé",
                    "en": "In the document or message used",
                    "es": "En el documento o mensaje utilizado",
                    "pt": "No documento ou mensagem utilizados",
                    "ar": "في الوثيقة أو الرسالة المستخدمة"
                  }
                },
                {
                  "code": 2,
                  "label": {
                    "fr": "Accessible par un lien ou une référence précise disponible",
                    "en": "Accessible through an available specific link or reference",
                    "es": "Accesible mediante un enlace o referencia precisa disponible",
                    "pt": "Acessível por uma ligação ou referência precisa disponível",
                    "ar": "متاحة عبر رابط أو مرجع دقيق"
                  }
                },
                {
                  "code": 0,
                  "label": {
                    "fr": "Absente",
                    "en": "Absent",
                    "es": "Ausente",
                    "pt": "Ausente",
                    "ar": "غير موجودة"
                  }
                },
                {
                  "code": 98,
                  "label": {
                    "fr": "Ne sait pas",
                    "en": "Do not know",
                    "es": "No sabe",
                    "pt": "Não sabe",
                    "ar": "لا أعرف"
                  }
                },
                {
                  "code": 99,
                  "label": {
                    "fr": "Non applicable",
                    "en": "Not applicable",
                    "es": "No aplicable",
                    "pt": "Não aplicável",
                    "ar": "لا ينطبق"
                  }
                }
              ],
              "source": "Rapport méthodologique du 15 novembre 2026, pp. 28–30, QU04",
              "gate": {
                "field": "QU01_Participation",
                "op": "in",
                "values": [
                  1
                ]
              }
            },
            {
              "id": "QU04_M6",
              "label": {
                "fr": "Population et territoire couverts",
                "en": "Population and geographical coverage",
                "es": "Población y territorio cubiertos",
                "pt": "População e território abrangidos",
                "ar": "السكان والنطاق الجغرافي المشمولان"
              },
              "type": "single",
              "options": [
                {
                  "code": 1,
                  "label": {
                    "fr": "Dans le document ou message utilisé",
                    "en": "In the document or message used",
                    "es": "En el documento o mensaje utilizado",
                    "pt": "No documento ou mensagem utilizados",
                    "ar": "في الوثيقة أو الرسالة المستخدمة"
                  }
                },
                {
                  "code": 2,
                  "label": {
                    "fr": "Accessible par un lien ou une référence précise disponible",
                    "en": "Accessible through an available specific link or reference",
                    "es": "Accesible mediante un enlace o referencia precisa disponible",
                    "pt": "Acessível por uma ligação ou referência precisa disponível",
                    "ar": "متاحة عبر رابط أو مرجع دقيق"
                  }
                },
                {
                  "code": 0,
                  "label": {
                    "fr": "Absente",
                    "en": "Absent",
                    "es": "Ausente",
                    "pt": "Ausente",
                    "ar": "غير موجودة"
                  }
                },
                {
                  "code": 98,
                  "label": {
                    "fr": "Ne sait pas",
                    "en": "Do not know",
                    "es": "No sabe",
                    "pt": "Não sabe",
                    "ar": "لا أعرف"
                  }
                },
                {
                  "code": 99,
                  "label": {
                    "fr": "Non applicable",
                    "en": "Not applicable",
                    "es": "No aplicable",
                    "pt": "Não aplicável",
                    "ar": "لا ينطبق"
                  }
                }
              ],
              "source": "Rapport méthodologique du 15 novembre 2026, pp. 28–30, QU04",
              "gate": {
                "field": "QU01_Participation",
                "op": "in",
                "values": [
                  1
                ]
              }
            },
            {
              "id": "QU04_M7",
              "label": {
                "fr": "Limites de comparaison, incertitudes ou précautions utiles",
                "en": "Comparability limitations, uncertainties or relevant cautions",
                "es": "Limitaciones de comparación, incertidumbres o precauciones pertinentes",
                "pt": "Limitações de comparação, incertezas ou precauções relevantes",
                "ar": "قيود المقارنة أو أوجه عدم اليقين أو الاحتياطات المفيدة"
              },
              "type": "single",
              "options": [
                {
                  "code": 1,
                  "label": {
                    "fr": "Dans le document ou message utilisé",
                    "en": "In the document or message used",
                    "es": "En el documento o mensaje utilizado",
                    "pt": "No documento ou mensagem utilizados",
                    "ar": "في الوثيقة أو الرسالة المستخدمة"
                  }
                },
                {
                  "code": 2,
                  "label": {
                    "fr": "Accessible par un lien ou une référence précise disponible",
                    "en": "Accessible through an available specific link or reference",
                    "es": "Accesible mediante un enlace o referencia precisa disponible",
                    "pt": "Acessível por uma ligação ou referência precisa disponível",
                    "ar": "متاحة عبر رابط أو مرجع دقيق"
                  }
                },
                {
                  "code": 0,
                  "label": {
                    "fr": "Absente",
                    "en": "Absent",
                    "es": "Ausente",
                    "pt": "Ausente",
                    "ar": "غير موجودة"
                  }
                },
                {
                  "code": 98,
                  "label": {
                    "fr": "Ne sait pas",
                    "en": "Do not know",
                    "es": "No sabe",
                    "pt": "Não sabe",
                    "ar": "لا أعرف"
                  }
                },
                {
                  "code": 99,
                  "label": {
                    "fr": "Non applicable",
                    "en": "Not applicable",
                    "es": "No aplicable",
                    "pt": "Não aplicável",
                    "ar": "لا ينطبق"
                  }
                }
              ],
              "source": "Rapport méthodologique du 15 novembre 2026, pp. 28–30, QU04",
              "gate": {
                "field": "QU01_Participation",
                "op": "in",
                "values": [
                  1
                ]
              }
            }
          ],
          "note": {
            "fr": "Pour le premier chiffre indiqué en QU03, quelles informations aviez-vous effectivement à disposition lors de la préparation ?",
            "en": "For the first figure identified in QU03, what information did you actually have available during preparation?",
            "es": "Para la primera cifra de QU03, ¿qué información tenía realmente disponible durante la preparación?",
            "pt": "Para o primeiro número de QU03, que informações tinha efetivamente disponíveis durante a preparação?",
            "ar": "بالنسبة لأول رقم في QU03، ما المعلومات التي كانت متاحة لديكم فعليا أثناء الإعداد؟"
          },
          "gate": {
            "all": [
              {
                "field": "QU02a",
                "op": "not_in",
                "values": [
                  0
                ]
              },
              {
                "field": "QU03_Cas",
                "op": "in",
                "values": [
                  1
                ]
              }
            ]
          },
          "theme": "Sources et métadonnées",
          "source": "Rapport méthodologique du 15 novembre 2026, pp. 28–30, QU04"
        },
        {
          "id": "QU05",
          "title": {
            "fr": "Vérifications avant diffusion de ce discours",
            "en": "Checks before dissemination of this speech",
            "es": "Comprobaciones antes de difundir este discurso",
            "pt": "Verificações antes da divulgação deste discurso",
            "ar": "المراجعات قبل نشر هذا الخطاب"
          },
          "fields": [
            {
              "id": "QU05a",
              "label": {
                "fr": "Qui a effectivement vérifié les chiffres et leur interprétation ? Plusieurs réponses possibles.",
                "en": "Who actually checked the figures and their interpretation? Multiple answers allowed.",
                "es": "¿Quién comprobó realmente las cifras y su interpretación? Varias respuestas posibles.",
                "pt": "Quem verificou efetivamente os números e a sua interpretação? Várias respostas possíveis.",
                "ar": "من راجع الأرقام وتفسيرها فعليا؟ يمكن اختيار عدة إجابات."
              },
              "type": "multi",
              "options": [
                {
                  "code": 1,
                  "label": {
                    "fr": "Rédacteur ou équipe de communication",
                    "en": "Speechwriter or communication team",
                    "es": "Redactor o equipo de comunicación",
                    "pt": "Redator ou equipa de comunicação",
                    "ar": "كاتب الخطاب أو فريق الاتصال"
                  }
                },
                {
                  "code": 2,
                  "label": {
                    "fr": "Conseiller technique ou statisticien de l’institution",
                    "en": "Institution’s technical adviser or statistician",
                    "es": "Asesor técnico o estadístico de la institución",
                    "pt": "Assessor técnico ou estatístico da instituição",
                    "ar": "مستشار فني أو إحصائي بالمؤسسة"
                  }
                },
                {
                  "code": 3,
                  "label": {
                    "fr": "Agent de l’INS",
                    "en": "NSO staff member",
                    "es": "Agente de la ONE",
                    "pt": "Agente do INE",
                    "ar": "موظف الجهاز الوطني للإحصاء"
                  }
                },
                {
                  "code": 4,
                  "label": {
                    "fr": "Autre organisme producteur",
                    "en": "Other producing organisation",
                    "es": "Otro organismo productor",
                    "pt": "Outro organismo produtor",
                    "ar": "جهة منتجة أخرى"
                  }
                },
                {
                  "code": 0,
                  "label": {
                    "fr": "Aucun contrôle connu",
                    "en": "No known check",
                    "es": "Ningún control conocido",
                    "pt": "Nenhum controlo conhecido",
                    "ar": "لا أعرف أي مراجعة"
                  }
                },
                {
                  "code": 97,
                  "label": {
                    "fr": "Autre fonction",
                    "en": "Other function",
                    "es": "Otra función",
                    "pt": "Outra função",
                    "ar": "وظيفة أخرى"
                  }
                },
                {
                  "code": 98,
                  "label": {
                    "fr": "Ne sait pas",
                    "en": "Do not know",
                    "es": "No sabe",
                    "pt": "Não sabe",
                    "ar": "لا أعرف"
                  }
                }
              ],
              "exclusive": [
                0,
                98
              ],
              "source": "Rapport méthodologique du 15 novembre 2026, pp. 28–30, QU05",
              "gate": {
                "field": "QU01_Participation",
                "op": "in",
                "values": [
                  1
                ]
              }
            },
            {
              "id": "QU05a_Autre",
              "label": {
                "fr": "Autre : précisez",
                "en": "Other: please specify",
                "es": "Otra : especifique",
                "pt": "Outra : especifique",
                "ar": "أخرى : يرجى التحديد"
              },
              "type": "text",
              "options": [],
              "max_chars": 500,
              "gate": {
                "all": [
                  {
                    "field": "QU01_Participation",
                    "op": "in",
                    "values": [
                      1
                    ]
                  },
                  {
                    "field": "QU05a",
                    "op": "contains",
                    "values": [
                      97
                    ]
                  }
                ]
              },
              "source": "Rapport méthodologique du 15 novembre 2026, pp. 28–30, QU05"
            },
            {
              "id": "QU05b",
              "label": {
                "fr": "L’INS a-t-il relu le passage contenant les chiffres dans le projet de discours ?",
                "en": "Did the NSO review the passage containing figures in the draft speech?",
                "es": "¿Revisó la ONE el pasaje con cifras del borrador del discurso?",
                "pt": "O INE reviu o trecho com números no projeto de discurso?",
                "ar": "هل راجع الجهاز الوطني للإحصاء المقطع المتضمن الأرقام في مشروع الخطاب؟"
              },
              "type": "single",
              "options": [
                {
                  "code": 1,
                  "label": {
                    "fr": "Oui",
                    "en": "Yes",
                    "es": "Sí",
                    "pt": "Sim",
                    "ar": "نعم"
                  }
                },
                {
                  "code": 2,
                  "label": {
                    "fr": "Non, il n’a pas été sollicité",
                    "en": "No, it was not asked",
                    "es": "No, no se le solicitó",
                    "pt": "Não, não foi solicitado",
                    "ar": "لا، لم يطلب منه ذلك"
                  }
                },
                {
                  "code": 3,
                  "label": {
                    "fr": "Non, il a été sollicité mais la relecture n’a pas eu lieu à temps",
                    "en": "No, it was asked but the review did not take place in time",
                    "es": "No, se solicitó pero la revisión no se realizó a tiempo",
                    "pt": "Não, foi solicitado mas a revisão não ocorreu a tempo",
                    "ar": "لا، طلب منه ذلك لكن المراجعة لم تتم في الوقت المناسب"
                  }
                },
                {
                  "code": 98,
                  "label": {
                    "fr": "Ne sait pas",
                    "en": "Do not know",
                    "es": "No sabe",
                    "pt": "Não sabe",
                    "ar": "لا أعرف"
                  }
                },
                {
                  "code": 99,
                  "label": {
                    "fr": "Non applicable, cette étape n’a pas eu lieu",
                    "en": "Not applicable, this stage did not occur",
                    "es": "No aplicable, esta etapa no tuvo lugar",
                    "pt": "Não aplicável, esta etapa não ocorreu",
                    "ar": "لا ينطبق، لم تتم هذه المرحلة"
                  }
                }
              ],
              "source": "Rapport méthodologique du 15 novembre 2026, pp. 28–30, QU05",
              "gate": {
                "field": "QU01_Participation",
                "op": "in",
                "values": [
                  1
                ]
              }
            },
            {
              "id": "QU05c",
              "label": {
                "fr": "Une référence à la source statistique figurait-elle dans la version diffusée ?",
                "en": "Did the disseminated version include a reference to the statistical source?",
                "es": "¿Incluía la versión difundida una referencia a la fuente estadística?",
                "pt": "A versão divulgada incluía uma referência à fonte estatística?",
                "ar": "هل تضمنت النسخة المنشورة إشارة إلى المصدر الإحصائي؟"
              },
              "type": "single",
              "options": [
                {
                  "code": 1,
                  "label": {
                    "fr": "Oui",
                    "en": "Yes",
                    "es": "Sí",
                    "pt": "Sim",
                    "ar": "نعم"
                  }
                },
                {
                  "code": 0,
                  "label": {
                    "fr": "Non",
                    "en": "No",
                    "es": "No",
                    "pt": "Não",
                    "ar": "لا"
                  }
                },
                {
                  "code": 98,
                  "label": {
                    "fr": "Ne sait pas",
                    "en": "Do not know",
                    "es": "No sabe",
                    "pt": "Não sabe",
                    "ar": "لا أعرف"
                  }
                },
                {
                  "code": 99,
                  "label": {
                    "fr": "Discours non diffusé ou version diffusée inconnue",
                    "en": "Speech not disseminated or disseminated version unknown",
                    "es": "Discurso no difundido o versión difundida desconocida",
                    "pt": "Discurso não divulgado ou versão divulgada desconhecida",
                    "ar": "الخطاب غير منشور أو النسخة المنشورة غير معروفة"
                  }
                }
              ],
              "source": "Rapport méthodologique du 15 novembre 2026, pp. 28–30, QU05",
              "gate": {
                "field": "QU01_Participation",
                "op": "in",
                "values": [
                  1
                ]
              }
            }
          ],
          "note": {
            "fr": "",
            "en": "",
            "es": "",
            "pt": "",
            "ar": ""
          },
          "gate": {
            "all": [
              {
                "field": "QU02a",
                "op": "not_in",
                "values": [
                  0
                ]
              },
              {
                "field": "QU03_Cas",
                "op": "in",
                "values": [
                  1
                ]
              }
            ]
          },
          "theme": "Vérification",
          "source": "Rapport méthodologique du 15 novembre 2026, pp. 28–30, QU05"
        },
        {
          "id": "QU06",
          "title": {
            "fr": "Corrections et retour à l’INS",
            "en": "Corrections and feedback to the NSO",
            "es": "Correcciones y comentarios a la ONE",
            "pt": "Correções e retorno ao INE",
            "ar": "التصحيحات والتغذية الراجعة للجهاز الوطني للإحصاء"
          },
          "fields": [
            {
              "id": "QU06a",
              "label": {
                "fr": "Pendant la période, avez-vous connaissance d’un chiffre ou d’une interprétation à corriger après finalisation d’un discours ?",
                "en": "During the period, do you know of a figure or interpretation requiring correction after a speech was finalised?",
                "es": "Durante el período, ¿conoce alguna cifra o interpretación que debiera corregirse después de finalizar un discurso?",
                "pt": "Durante o período, conhece algum número ou interpretação que exigisse correção após a finalização de um discurso?",
                "ar": "هل تعرف رقما أو تفسيرا استلزم التصحيح بعد إنهاء إعداد خطاب خلال الفترة؟"
              },
              "type": "single",
              "options": [
                {
                  "code": 1,
                  "label": {
                    "fr": "Oui, correction avant diffusion",
                    "en": "Yes, correction before dissemination",
                    "es": "Sí, corrección antes de la difusión",
                    "pt": "Sim, correção antes da divulgação",
                    "ar": "نعم، تصحيح قبل النشر"
                  }
                },
                {
                  "code": 2,
                  "label": {
                    "fr": "Oui, correction après diffusion",
                    "en": "Yes, correction after dissemination",
                    "es": "Sí, corrección después de la difusión",
                    "pt": "Sim, correção após a divulgação",
                    "ar": "نعم، تصحيح بعد النشر"
                  }
                },
                {
                  "code": 3,
                  "label": {
                    "fr": "Les deux situations",
                    "en": "Both situations",
                    "es": "Ambas situaciones",
                    "pt": "Ambas as situações",
                    "ar": "كلتا الحالتين"
                  }
                },
                {
                  "code": 0,
                  "label": {
                    "fr": "Aucun épisode connu",
                    "en": "No known instance",
                    "es": "Ningún episodio conocido",
                    "pt": "Nenhum episódio conhecido",
                    "ar": "لا أعرف أي حالة"
                  }
                },
                {
                  "code": 98,
                  "label": {
                    "fr": "Ne sait pas",
                    "en": "Do not know",
                    "es": "No sabe",
                    "pt": "Não sabe",
                    "ar": "لا أعرف"
                  }
                },
                {
                  "code": 99,
                  "label": {
                    "fr": "Aucun discours concerné",
                    "en": "No relevant speech",
                    "es": "Ningún discurso pertinente",
                    "pt": "Nenhum discurso em causa",
                    "ar": "لا يوجد خطاب معني"
                  }
                }
              ],
              "source": "Rapport méthodologique du 15 novembre 2026, pp. 28–30, QU06",
              "gate": {
                "field": "QU01_Participation",
                "op": "in",
                "values": [
                  1
                ]
              }
            },
            {
              "id": "QU06_Actions",
              "label": {
                "fr": "Pour le cas le plus récent, quelles actions ont été réalisées ? Plusieurs réponses possibles.",
                "en": "For the most recent case, what actions were taken? Multiple answers allowed.",
                "es": "En el caso más reciente, ¿qué acciones se realizaron? Se admiten varias respuestas.",
                "pt": "No caso mais recente, que ações foram realizadas? São admitidas várias respostas.",
                "ar": "ما الإجراءات المتخذة في أحدث حالة؟ يمكن اختيار عدة إجابات."
              },
              "type": "multi",
              "options": [
                {
                  "code": 1,
                  "label": {
                    "fr": "Retour à la source ou au producteur",
                    "en": "Returning to the source or producer",
                    "es": "Consulta de la fuente o del productor",
                    "pt": "Consulta da fonte ou do produtor",
                    "ar": "الرجوع إلى المصدر أو المنتج"
                  }
                },
                {
                  "code": 2,
                  "label": {
                    "fr": "Version corrigée transmise à l’équipe",
                    "en": "Corrected version sent to the team",
                    "es": "Versión corregida enviada al equipo",
                    "pt": "Versão corrigida enviada à equipa",
                    "ar": "إرسال نسخة مصححة إلى الفريق"
                  }
                },
                {
                  "code": 3,
                  "label": {
                    "fr": "Conservation du changement et de son motif",
                    "en": "Keeping a record of the change and its reason",
                    "es": "Conservación del cambio y de su motivo",
                    "pt": "Conservação da alteração e do motivo",
                    "ar": "حفظ التغيير وسببه"
                  }
                },
                {
                  "code": 4,
                  "label": {
                    "fr": "Rectificatif diffusé pour une erreur déjà publique",
                    "en": "Correction issued for an already public error",
                    "es": "Rectificación difundida de un error ya público",
                    "pt": "Retificação divulgada de um erro já público",
                    "ar": "نشر تصحيح لخطأ أصبح علنيا"
                  }
                },
                {
                  "code": 5,
                  "label": {
                    "fr": "Vérification de la prise en compte",
                    "en": "Checking that the correction was incorporated",
                    "es": "Verificación de la incorporación de la corrección",
                    "pt": "Verificação da incorporação da correção",
                    "ar": "التحقق من إدراج التصحيح"
                  }
                },
                {
                  "code": 0,
                  "label": {
                    "fr": "Aucune action connue",
                    "en": "No known action",
                    "es": "Ninguna acción conocida",
                    "pt": "Nenhuma ação conhecida",
                    "ar": "لا أعرف أي إجراء"
                  }
                },
                {
                  "code": 97,
                  "label": {
                    "fr": "Autre",
                    "en": "Other",
                    "es": "Otra",
                    "pt": "Outra",
                    "ar": "أخرى"
                  }
                },
                {
                  "code": 98,
                  "label": {
                    "fr": "Ne sait pas",
                    "en": "Do not know",
                    "es": "No sabe",
                    "pt": "Não sabe",
                    "ar": "لا أعرف"
                  }
                }
              ],
              "exclusive": [
                0,
                98
              ],
              "gate": {
                "all": [
                  {
                    "field": "QU01_Participation",
                    "op": "in",
                    "values": [
                      1
                    ]
                  },
                  {
                    "field": "QU06a",
                    "op": "in",
                    "values": [
                      1,
                      2,
                      3
                    ]
                  }
                ]
              },
              "source": "Rapport méthodologique du 15 novembre 2026, pp. 28–30, QU06"
            },
            {
              "id": "QU06_Actions_Autre",
              "label": {
                "fr": "Autre : précisez",
                "en": "Other: please specify",
                "es": "Otra : especifique",
                "pt": "Outra : especifique",
                "ar": "أخرى : يرجى التحديد"
              },
              "type": "text",
              "options": [],
              "max_chars": 500,
              "gate": {
                "all": [
                  {
                    "field": "QU01_Participation",
                    "op": "in",
                    "values": [
                      1
                    ]
                  },
                  {
                    "field": "QU06_Actions",
                    "op": "contains",
                    "values": [
                      97
                    ]
                  }
                ]
              },
              "source": "Rapport méthodologique du 15 novembre 2026, pp. 28–30, QU06"
            },
            {
              "id": "QU06b",
              "label": {
                "fr": "Pour le dernier appui demandé à l’INS pendant la période, lui avez-vous transmis un retour sur l’utilisation des chiffres ?",
                "en": "For the most recent support requested from the NSO during the period, did you provide feedback on the use of the figures?",
                "es": "Para el último apoyo solicitado a la ONE durante el período, ¿le transmitió comentarios sobre el uso de las cifras?",
                "pt": "Para o último apoio solicitado ao INE durante o período, transmitiu-lhe retorno sobre a utilização dos números?",
                "ar": "بالنسبة لآخر دعم طلب من الجهاز الوطني للإحصاء خلال الفترة، هل قدمتم له ملاحظات عن استخدام الأرقام؟"
              },
              "type": "single",
              "options": [
                {
                  "code": 1,
                  "label": {
                    "fr": "Oui, texte ou lien vers le discours",
                    "en": "Yes, text or a link to the speech",
                    "es": "Sí, texto o enlace al discurso",
                    "pt": "Sim, texto ou ligação para o discurso",
                    "ar": "نعم، نص الخطاب أو رابطه"
                  }
                },
                {
                  "code": 2,
                  "label": {
                    "fr": "Oui, commentaire sur l’appui, sans texte ni lien",
                    "en": "Yes, comments on the support, without text or link",
                    "es": "Sí, comentario sobre el apoyo, sin texto ni enlace",
                    "pt": "Sim, comentário sobre o apoio, sem texto nem ligação",
                    "ar": "نعم، تعليق على الدعم دون نص أو رابط"
                  }
                },
                {
                  "code": 3,
                  "label": {
                    "fr": "Les deux",
                    "en": "Both",
                    "es": "Ambos",
                    "pt": "Ambos",
                    "ar": "كلاهما"
                  }
                },
                {
                  "code": 0,
                  "label": {
                    "fr": "Non",
                    "en": "No",
                    "es": "No",
                    "pt": "Não",
                    "ar": "لا"
                  }
                },
                {
                  "code": 98,
                  "label": {
                    "fr": "Ne sait pas",
                    "en": "Do not know",
                    "es": "No sabe",
                    "pt": "Não sabe",
                    "ar": "لا أعرف"
                  }
                },
                {
                  "code": 99,
                  "label": {
                    "fr": "Aucun appui demandé",
                    "en": "No support requested",
                    "es": "No se solicitó apoyo",
                    "pt": "Não foi solicitado apoio",
                    "ar": "لم يطلب دعم"
                  }
                }
              ],
              "source": "Rapport méthodologique du 15 novembre 2026, pp. 28–30, QU06",
              "gate": {
                "field": "QU01_Participation",
                "op": "in",
                "values": [
                  1
                ]
              }
            }
          ],
          "note": {
            "fr": "",
            "en": "",
            "es": "",
            "pt": "",
            "ar": ""
          },
          "gate": {
            "all": [
              {
                "field": "QU02a",
                "op": "not_in",
                "values": [
                  0
                ]
              },
              {
                "field": "QU03_Cas",
                "op": "in",
                "values": [
                  1
                ]
              }
            ]
          },
          "theme": "Actualisation et correction",
          "source": "Rapport méthodologique du 15 novembre 2026, pp. 28–30, QU06"
        },
        {
          "id": "QU07",
          "title": {
            "fr": "Appuis les plus utiles",
            "en": "Most useful forms of support",
            "es": "Apoyos más útiles",
            "pt": "Apoios mais úteis",
            "ar": "أكثر أوجه الدعم فائدة"
          },
          "fields": [
            {
              "id": "QU07_P1",
              "label": {
                "fr": "Priorité 1",
                "en": "Priority 1",
                "es": "Prioridad 1",
                "pt": "Prioridade 1",
                "ar": "الأولوية 1"
              },
              "type": "single",
              "options": [
                {
                  "code": 1,
                  "label": {
                    "fr": "Contact clairement identifié à l’INS",
                    "en": "Clearly identified contact at the NSO",
                    "es": "Contacto claramente identificado en la ONE",
                    "pt": "Contacto claramente identificado no INE",
                    "ar": "جهة اتصال محددة بوضوح في الجهاز الوطني للإحصاء"
                  }
                },
                {
                  "code": 2,
                  "label": {
                    "fr": "Réponse dans un délai convenu, y compris en urgence",
                    "en": "Response within an agreed deadline, including urgent requests",
                    "es": "Respuesta en un plazo convenido, también en urgencias",
                    "pt": "Resposta num prazo acordado, incluindo urgências",
                    "ar": "إجابة خلال مهلة متفق عليها، بما فيها الحالات العاجلة"
                  }
                },
                {
                  "code": 3,
                  "label": {
                    "fr": "Fiche courte avec chiffre, source et période",
                    "en": "Short fact sheet with figure, source and period",
                    "es": "Ficha breve con cifra, fuente y período",
                    "pt": "Ficha breve com número, fonte e período",
                    "ar": "بطاقة موجزة تتضمن الرقم والمصدر والفترة"
                  }
                },
                {
                  "code": 4,
                  "label": {
                    "fr": "Explication des définitions et limites",
                    "en": "Explanation of definitions and limitations",
                    "es": "Explicación de definiciones y limitaciones",
                    "pt": "Explicação de definições e limitações",
                    "ar": "شرح التعريفات والقيود"
                  }
                },
                {
                  "code": 5,
                  "label": {
                    "fr": "Proposition de phrases compréhensibles",
                    "en": "Suggested clear wording",
                    "es": "Propuesta de frases comprensibles",
                    "pt": "Proposta de frases compreensíveis",
                    "ar": "اقتراح صياغات مفهومة"
                  }
                },
                {
                  "code": 6,
                  "label": {
                    "fr": "Relecture rapide du passage statistique",
                    "en": "Quick review of the statistical passage",
                    "es": "Revisión rápida del pasaje estadístico",
                    "pt": "Revisão rápida do trecho estatístico",
                    "ar": "مراجعة سريعة للمقطع الإحصائي"
                  }
                },
                {
                  "code": 7,
                  "label": {
                    "fr": "Alerte en cas de révision ou d’erreur",
                    "en": "Alert in the event of a revision or error",
                    "es": "Aviso en caso de revisión o error",
                    "pt": "Alerta em caso de revisão ou erro",
                    "ar": "تنبيه عند التنقيح أو الخطأ"
                  }
                },
                {
                  "code": 8,
                  "label": {
                    "fr": "Exemples et courte formation",
                    "en": "Examples and brief training",
                    "es": "Ejemplos y formación breve",
                    "pt": "Exemplos e formação breve",
                    "ar": "أمثلة وتدريب قصير"
                  }
                },
                {
                  "code": 97,
                  "label": {
                    "fr": "Autre",
                    "en": "Other",
                    "es": "Otra",
                    "pt": "Outra",
                    "ar": "أخرى"
                  }
                },
                {
                  "code": 0,
                  "label": {
                    "fr": "Aucun besoin exprimé",
                    "en": "No need expressed",
                    "es": "Ninguna necesidad expresada",
                    "pt": "Nenhuma necessidade expressa",
                    "ar": "لم يعبر عن أي احتياج"
                  }
                },
                {
                  "code": 98,
                  "label": {
                    "fr": "Ne sait pas",
                    "en": "Do not know",
                    "es": "No sabe",
                    "pt": "Não sabe",
                    "ar": "لا أعرف"
                  }
                }
              ],
              "source": "Rapport méthodologique du 15 novembre 2026, pp. 28–30, QU07",
              "gate": {
                "field": "QU01_Participation",
                "op": "in",
                "values": [
                  1
                ]
              }
            },
            {
              "id": "QU07_P2",
              "label": {
                "fr": "Priorité 2",
                "en": "Priority 2",
                "es": "Prioridad 2",
                "pt": "Prioridade 2",
                "ar": "الأولوية 2"
              },
              "type": "single",
              "options": [
                {
                  "code": 1,
                  "label": {
                    "fr": "Contact clairement identifié à l’INS",
                    "en": "Clearly identified contact at the NSO",
                    "es": "Contacto claramente identificado en la ONE",
                    "pt": "Contacto claramente identificado no INE",
                    "ar": "جهة اتصال محددة بوضوح في الجهاز الوطني للإحصاء"
                  }
                },
                {
                  "code": 2,
                  "label": {
                    "fr": "Réponse dans un délai convenu, y compris en urgence",
                    "en": "Response within an agreed deadline, including urgent requests",
                    "es": "Respuesta en un plazo convenido, también en urgencias",
                    "pt": "Resposta num prazo acordado, incluindo urgências",
                    "ar": "إجابة خلال مهلة متفق عليها، بما فيها الحالات العاجلة"
                  }
                },
                {
                  "code": 3,
                  "label": {
                    "fr": "Fiche courte avec chiffre, source et période",
                    "en": "Short fact sheet with figure, source and period",
                    "es": "Ficha breve con cifra, fuente y período",
                    "pt": "Ficha breve com número, fonte e período",
                    "ar": "بطاقة موجزة تتضمن الرقم والمصدر والفترة"
                  }
                },
                {
                  "code": 4,
                  "label": {
                    "fr": "Explication des définitions et limites",
                    "en": "Explanation of definitions and limitations",
                    "es": "Explicación de definiciones y limitaciones",
                    "pt": "Explicação de definições e limitações",
                    "ar": "شرح التعريفات والقيود"
                  }
                },
                {
                  "code": 5,
                  "label": {
                    "fr": "Proposition de phrases compréhensibles",
                    "en": "Suggested clear wording",
                    "es": "Propuesta de frases comprensibles",
                    "pt": "Proposta de frases compreensíveis",
                    "ar": "اقتراح صياغات مفهومة"
                  }
                },
                {
                  "code": 6,
                  "label": {
                    "fr": "Relecture rapide du passage statistique",
                    "en": "Quick review of the statistical passage",
                    "es": "Revisión rápida del pasaje estadístico",
                    "pt": "Revisão rápida do trecho estatístico",
                    "ar": "مراجعة سريعة للمقطع الإحصائي"
                  }
                },
                {
                  "code": 7,
                  "label": {
                    "fr": "Alerte en cas de révision ou d’erreur",
                    "en": "Alert in the event of a revision or error",
                    "es": "Aviso en caso de revisión o error",
                    "pt": "Alerta em caso de revisão ou erro",
                    "ar": "تنبيه عند التنقيح أو الخطأ"
                  }
                },
                {
                  "code": 8,
                  "label": {
                    "fr": "Exemples et courte formation",
                    "en": "Examples and brief training",
                    "es": "Ejemplos y formación breve",
                    "pt": "Exemplos e formação breve",
                    "ar": "أمثلة وتدريب قصير"
                  }
                },
                {
                  "code": 97,
                  "label": {
                    "fr": "Autre",
                    "en": "Other",
                    "es": "Otra",
                    "pt": "Outra",
                    "ar": "أخرى"
                  }
                }
              ],
              "gate": {
                "all": [
                  {
                    "field": "QU01_Participation",
                    "op": "in",
                    "values": [
                      1
                    ]
                  },
                  {
                    "field": "QU07_P1",
                    "op": "in",
                    "values": [
                      1,
                      2,
                      3,
                      4,
                      5,
                      6,
                      7,
                      8,
                      97
                    ]
                  }
                ]
              },
              "source": "Rapport méthodologique du 15 novembre 2026, pp. 28–30, QU07"
            },
            {
              "id": "QU07_P3",
              "label": {
                "fr": "Priorité 3",
                "en": "Priority 3",
                "es": "Prioridad 3",
                "pt": "Prioridade 3",
                "ar": "الأولوية 3"
              },
              "type": "single",
              "options": [
                {
                  "code": 1,
                  "label": {
                    "fr": "Contact clairement identifié à l’INS",
                    "en": "Clearly identified contact at the NSO",
                    "es": "Contacto claramente identificado en la ONE",
                    "pt": "Contacto claramente identificado no INE",
                    "ar": "جهة اتصال محددة بوضوح في الجهاز الوطني للإحصاء"
                  }
                },
                {
                  "code": 2,
                  "label": {
                    "fr": "Réponse dans un délai convenu, y compris en urgence",
                    "en": "Response within an agreed deadline, including urgent requests",
                    "es": "Respuesta en un plazo convenido, también en urgencias",
                    "pt": "Resposta num prazo acordado, incluindo urgências",
                    "ar": "إجابة خلال مهلة متفق عليها، بما فيها الحالات العاجلة"
                  }
                },
                {
                  "code": 3,
                  "label": {
                    "fr": "Fiche courte avec chiffre, source et période",
                    "en": "Short fact sheet with figure, source and period",
                    "es": "Ficha breve con cifra, fuente y período",
                    "pt": "Ficha breve com número, fonte e período",
                    "ar": "بطاقة موجزة تتضمن الرقم والمصدر والفترة"
                  }
                },
                {
                  "code": 4,
                  "label": {
                    "fr": "Explication des définitions et limites",
                    "en": "Explanation of definitions and limitations",
                    "es": "Explicación de definiciones y limitaciones",
                    "pt": "Explicação de definições e limitações",
                    "ar": "شرح التعريفات والقيود"
                  }
                },
                {
                  "code": 5,
                  "label": {
                    "fr": "Proposition de phrases compréhensibles",
                    "en": "Suggested clear wording",
                    "es": "Propuesta de frases comprensibles",
                    "pt": "Proposta de frases compreensíveis",
                    "ar": "اقتراح صياغات مفهومة"
                  }
                },
                {
                  "code": 6,
                  "label": {
                    "fr": "Relecture rapide du passage statistique",
                    "en": "Quick review of the statistical passage",
                    "es": "Revisión rápida del pasaje estadístico",
                    "pt": "Revisão rápida do trecho estatístico",
                    "ar": "مراجعة سريعة للمقطع الإحصائي"
                  }
                },
                {
                  "code": 7,
                  "label": {
                    "fr": "Alerte en cas de révision ou d’erreur",
                    "en": "Alert in the event of a revision or error",
                    "es": "Aviso en caso de revisión o error",
                    "pt": "Alerta em caso de revisão ou erro",
                    "ar": "تنبيه عند التنقيح أو الخطأ"
                  }
                },
                {
                  "code": 8,
                  "label": {
                    "fr": "Exemples et courte formation",
                    "en": "Examples and brief training",
                    "es": "Ejemplos y formación breve",
                    "pt": "Exemplos e formação breve",
                    "ar": "أمثلة وتدريب قصير"
                  }
                },
                {
                  "code": 97,
                  "label": {
                    "fr": "Autre",
                    "en": "Other",
                    "es": "Otra",
                    "pt": "Outra",
                    "ar": "أخرى"
                  }
                }
              ],
              "gate": {
                "all": [
                  {
                    "field": "QU01_Participation",
                    "op": "in",
                    "values": [
                      1
                    ]
                  },
                  {
                    "field": "QU07_P1",
                    "op": "in",
                    "values": [
                      1,
                      2,
                      3,
                      4,
                      5,
                      6,
                      7,
                      8,
                      97
                    ]
                  }
                ]
              },
              "source": "Rapport méthodologique du 15 novembre 2026, pp. 28–30, QU07"
            },
            {
              "id": "QU07_Autre",
              "label": {
                "fr": "Autre priorité : précisez",
                "en": "Other priority: please specify",
                "es": "Otra prioridad : especifique",
                "pt": "Outra prioridade : especifique",
                "ar": "أولوية أخرى : يرجى التحديد"
              },
              "type": "text",
              "options": [],
              "max_chars": 500,
              "gate": {
                "all": [
                  {
                    "field": "QU01_Participation",
                    "op": "in",
                    "values": [
                      1
                    ]
                  },
                  {
                    "any": [
                      {
                        "field": "QU07_P1",
                        "op": "in",
                        "values": [
                          97
                        ]
                      },
                      {
                        "field": "QU07_P2",
                        "op": "in",
                        "values": [
                          97
                        ]
                      },
                      {
                        "field": "QU07_P3",
                        "op": "in",
                        "values": [
                          97
                        ]
                      }
                    ]
                  }
                ]
              },
              "source": "Rapport méthodologique du 15 novembre 2026, pp. 28–30, QU07"
            },
            {
              "id": "QU07_Difficulte",
              "label": {
                "fr": "Pour la priorité 1, citer une difficulté effectivement rencontrée, au plus 40 mots. À défaut, écrire « besoin anticipé ».",
                "en": "For priority 1, describe a difficulty actually experienced, up to 40 words. Otherwise write ‘anticipated need’.",
                "es": "Para la prioridad 1, describa una dificultad real en un máximo de 40 palabras. En su defecto, escriba « necesidad prevista ».",
                "pt": "Para a prioridade 1, descreva uma dificuldade real em até 40 palavras. Caso contrário, escreva « necessidade prevista ».",
                "ar": "بالنسبة للأولوية 1، صف صعوبة فعلية في حدود 40 كلمة، أو اكتب « احتياج متوقع »."
              },
              "type": "textarea",
              "options": [],
              "max_chars": 2500,
              "max_words": 40,
              "source": "Rapport méthodologique du 15 novembre 2026, pp. 28–30, QU07",
              "gate": {
                "field": "QU01_Participation",
                "op": "in",
                "values": [
                  1
                ]
              }
            },
            {
              "id": "QU07_Contrainte",
              "label": {
                "fr": "Contrainte particulière liée au public, à la langue ou au contexte de communication, facultatif ; au plus 40 mots, aucune opinion politique demandée",
                "en": "Specific constraint related to the audience, language or communication context, optional; up to 40 words, no political opinion requested",
                "es": "Restricción relacionada con el público, el idioma o el contexto de comunicación, opcional; máximo 40 palabras, sin opinión política",
                "pt": "Restrição ligada ao público, à língua ou ao contexto de comunicação, facultativa; máximo de 40 palavras, sem opinião política",
                "ar": "قيد يتعلق بالجمهور أو اللغة أو سياق الاتصال، اختياري؛ حتى 40 كلمة دون رأي سياسي"
              },
              "type": "textarea",
              "options": [],
              "max_chars": 2500,
              "max_words": 40,
              "source": "Rapport méthodologique du 15 novembre 2026, pp. 28–30, QU07",
              "gate": {
                "field": "QU01_Participation",
                "op": "in",
                "values": [
                  1
                ]
              }
            },
            {
              "id": "QU07_Nouveaute",
              "label": {
                "fr": "Pratique nouvelle après le 31 octobre, facultatif : changement et date",
                "en": "New practice after 31 October, optional: change and date",
                "es": "Práctica nueva después del 31 de octubre, opcional : cambio y fecha",
                "pt": "Prática nova após 31 de outubro, facultativo : alteração e data",
                "ar": "ممارسة جديدة بعد 31 أكتوبر، اختياري : التغيير وتاريخه"
              },
              "type": "textarea",
              "options": [],
              "max_chars": 2500,
              "source": "Rapport méthodologique du 15 novembre 2026, pp. 28–30, QU07",
              "gate": {
                "field": "QU01_Participation",
                "op": "in",
                "values": [
                  1
                ]
              }
            }
          ],
          "note": {
            "fr": "Classer au plus trois codes différents. Les choix 0 et 98 excluent les autres priorités.",
            "en": "Rank up to three different codes. Options 0 and 98 exclude other priorities.",
            "es": "Ordene hasta tres códigos distintos. Las opciones 0 y 98 excluyen las demás prioridades.",
            "pt": "Ordene até três códigos distintos. As opções 0 e 98 excluem as restantes prioridades.",
            "ar": "رتب ثلاثة رموز مختلفة كحد أقصى. يستبعد الخياران 0 و98 بقية الأولويات."
          },
          "gate": null,
          "theme": "Suivi et moyens",
          "source": "Rapport méthodologique du 15 novembre 2026, pp. 28–30, QU07"
        },
        {
          "id": "QU08",
          "title": {
            "fr": "Attribution et contact facultatif",
            "en": "Attribution and optional contact",
            "es": "Atribución y contacto opcional",
            "pt": "Atribuição e contacto facultativo",
            "ar": "الإسناد وبيانات الاتصال الاختيارية"
          },
          "fields": [
            {
              "id": "QU08_Attribution",
              "label": {
                "fr": "Modalité autorisée pour la contribution et le cas partagé",
                "en": "Permitted use of the contribution and shared case",
                "es": "Uso autorizado de la contribución y del caso compartido",
                "pt": "Utilização autorizada da contribuição e do caso partilhado",
                "ar": "الاستخدام المصرح به للمساهمة والحالة المشتركة"
              },
              "type": "single",
              "options": [
                {
                  "code": 0,
                  "label": {
                    "fr": "Synthèse anonymisée seulement, sans attribution identifiable",
                    "en": "Anonymised summary only, with no identifiable attribution",
                    "es": "Solo síntesis anonimizada, sin atribución identificable",
                    "pt": "Apenas síntese anonimizada, sem atribuição identificável",
                    "ar": "ملخص دون تحديد الهوية فقط، دون إسناد كاشف للهوية"
                  }
                },
                {
                  "code": 1,
                  "label": {
                    "fr": "Attribution à l’institution, sans citation textuelle identifiable",
                    "en": "Attribution to the institution, without an identifiable quotation",
                    "es": "Atribución a la institución, sin cita textual identificable",
                    "pt": "Atribuição à instituição, sem citação textual identificável",
                    "ar": "الإسناد إلى المؤسسة دون اقتباس نصي كاشف للهوية"
                  }
                },
                {
                  "code": 2,
                  "label": {
                    "fr": "Attribution à l’institution et citation textuelle",
                    "en": "Attribution to the institution and direct quotation",
                    "es": "Atribución a la institución y cita textual",
                    "pt": "Atribuição à instituição e citação textual",
                    "ar": "الإسناد إلى المؤسسة والاقتباس النصي"
                  }
                },
                {
                  "code": 3,
                  "label": {
                    "fr": "Me soumettre le passage avant toute attribution ou citation identifiable",
                    "en": "Submit the passage to me before any identifiable attribution or quotation",
                    "es": "Enviarme el pasaje antes de cualquier atribución o cita identificable",
                    "pt": "Enviar-me o trecho antes de qualquer atribuição ou citação identificável",
                    "ar": "اعرض علي المقطع قبل أي إسناد أو اقتباس كاشف للهوية"
                  }
                }
              ],
              "attribution": true,
              "source": "Rapport méthodologique du 15 novembre 2026, pp. 28–30, QU08",
              "gate": {
                "field": "QU01_Participation",
                "op": "in",
                "values": [
                  1
                ]
              }
            },
            {
              "id": "QU08_Suite",
              "label": {
                "fr": "Acceptez-vous un complément ou un test ?",
                "en": "Do you agree to follow-up or a test?",
                "es": "¿Acepta una aclaración o una prueba?",
                "pt": "Aceita um esclarecimento ou um teste?",
                "ar": "هل توافق على استكمال أو اختبار؟"
              },
              "type": "single",
              "options": [
                {
                  "code": 1,
                  "label": {
                    "fr": "Complément",
                    "en": "Follow-up clarification",
                    "es": "Complemento",
                    "pt": "Complemento",
                    "ar": "استكمال"
                  }
                },
                {
                  "code": 2,
                  "label": {
                    "fr": "Test",
                    "en": "Tool test",
                    "es": "Prueba de herramienta",
                    "pt": "Teste de ferramenta",
                    "ar": "اختبار أداة"
                  }
                },
                {
                  "code": 3,
                  "label": {
                    "fr": "Les deux",
                    "en": "Both",
                    "es": "Ambos",
                    "pt": "Ambos",
                    "ar": "كلاهما"
                  }
                },
                {
                  "code": 0,
                  "label": {
                    "fr": "Non",
                    "en": "No",
                    "es": "No",
                    "pt": "Não",
                    "ar": "لا"
                  }
                },
                {
                  "code": 98,
                  "label": {
                    "fr": "À déterminer",
                    "en": "To be determined",
                    "es": "Por determinar",
                    "pt": "A determinar",
                    "ar": "يحدد لاحقا"
                  }
                }
              ],
              "source": "Rapport méthodologique du 15 novembre 2026, pp. 28–30, QU08",
              "gate": {
                "field": "QU01_Participation",
                "op": "in",
                "values": [
                  1
                ]
              }
            },
            {
              "id": "QU08_Contact_fonction",
              "label": {
                "fr": "Contact facultatif : fonction ou service à recontacter",
                "en": "Optional contact: function or service to contact",
                "es": "Contacto opcional : función o servicio al que contactar",
                "pt": "Contacto facultativo : função ou serviço a contactar",
                "ar": "جهة اتصال اختيارية : الوظيفة أو الإدارة التي يمكن التواصل معها"
              },
              "type": "text",
              "options": [],
              "max_chars": 500,
              "private": true,
              "source": "Rapport méthodologique du 15 novembre 2026, pp. 28–30, QU08",
              "gate": {
                "field": "QU01_Participation",
                "op": "in",
                "values": [
                  1
                ]
              }
            },
            {
              "id": "QU08_Contact_courriel",
              "label": {
                "fr": "Courriel institutionnel ou générique, facultatif",
                "en": "Institutional or generic email address, optional",
                "es": "Correo institucional o genérico, opcional",
                "pt": "Correio institucional ou genérico, facultativo",
                "ar": "بريد مؤسسي أو عام، اختياري"
              },
              "type": "email",
              "options": [],
              "max_chars": 254,
              "private": true,
              "source": "Rapport méthodologique du 15 novembre 2026, pp. 28–30, QU08",
              "gate": {
                "field": "QU01_Participation",
                "op": "in",
                "values": [
                  1
                ]
              }
            },
            {
              "id": "QU08_Contact_relais",
              "label": {
                "fr": "Préférer un relais par STATAFRIC, facultatif",
                "en": "Prefer contact through STATAFRIC, optional",
                "es": "Preferir contacto a través de STATAFRIC, opcional",
                "pt": "Preferir contacto através do STATAFRIC, facultativo",
                "ar": "تفضيل التواصل عبر STATAFRIC، اختياري"
              },
              "type": "single",
              "options": [
                {
                  "code": 1,
                  "label": {
                    "fr": "Passer par STATAFRIC",
                    "en": "Contact through STATAFRIC",
                    "es": "Contacto a través de STATAFRIC",
                    "pt": "Contacto através do STATAFRIC",
                    "ar": "التواصل عبر STATAFRIC"
                  }
                }
              ],
              "private": true,
              "source": "Rapport méthodologique du 15 novembre 2026, pp. 28–30, QU08",
              "gate": {
                "field": "QU01_Participation",
                "op": "in",
                "values": [
                  1
                ]
              }
            }
          ],
          "note": {
            "fr": "Les choix 1 et 2 supposent l’autorisation correspondante. Une contribution individuelle sans cette autorisation reste anonymisée. Sans choix explicite, la synthèse anonymisée (code 0) s’applique. L’absence de retour ne vaut jamais accord de citation.",
            "en": "Options 1 and 2 require the corresponding authorisation. An individual contribution without that authorisation remains anonymised. Without an explicit choice, an anonymised summary (code 0) applies. Silence never constitutes permission to quote.",
            "es": "Las opciones 1 y 2 requieren la autorización correspondiente. Una contribución individual sin esa autorización permanece anonimizada. Sin selección explícita, se aplica la síntesis anonimizada (código 0). El silencio nunca autoriza una cita.",
            "pt": "As opções 1 e 2 exigem a autorização correspondente. Uma contribuição individual sem essa autorização mantém-se anonimizada. Sem escolha explícita, aplica-se a síntese anonimizada (código 0). O silêncio nunca autoriza uma citação.",
            "ar": "يتطلب الخياران 1 و2 التصريح المناسب. تبقى المساهمة الفردية دون هذا التصريح مجهولة الهوية. عند غياب اختيار صريح، يطبق الملخص دون تحديد الهوية (الرمز 0). الصمت لا يعني أبدا الإذن بالاقتباس."
          },
          "gate": null,
          "theme": "Suivi et moyens",
          "source": "Rapport méthodologique du 15 novembre 2026, pp. 28–30, QU08"
        }
      ],
      "consent": "QU01_Participation",
      "attribution": "QU08_Attribution",
      "institution": "QU01_Institution",
      "country": "QU01_Pays",
      "status": "QU01_Statut",
      "previous": "QU01_Reference_precedente",
      "version": "1.2 — 2026-11-15",
      "unit": "QU01_Unite",
      "sector": "QU01_Domaine",
      "response_language": "QU01_Langue_texte",
      "origin": "QU01_Origine_invitation",
      "relay": "QU01_INS_relais",
      "profile": "Équipe de discours",
      "priority_prefix": "QU07"
    },
    "partenaires": {
      "title": {
        "fr": "CER et organisations internationales",
        "en": "RECs and international organisations",
        "es": "CER y organizaciones internacionales",
        "pt": "CER e organizações internacionais",
        "ar": "الجماعات الاقتصادية الإقليمية والمنظمات الدولية"
      },
      "duration": {
        "fr": "10 à 15 minutes",
        "en": "10 to 15 minutes",
        "es": "De 10 a 15 minutos",
        "pt": "10 a 15 minutos",
        "ar": "من 10 إلى 15 دقيقة"
      },
      "sections": [
        {
          "id": "QR01",
          "title": {
            "fr": "Participation et champ de la contribution",
            "en": "Participation and scope of the contribution",
            "es": "Participación y ámbito de la contribución",
            "pt": "Participação e âmbito da contribuição",
            "ar": "المشاركة ونطاق المساهمة"
          },
          "fields": [
            {
              "id": "QR01_Participation",
              "label": {
                "fr": "Acceptez-vous l’utilisation de votre contribution selon la notice ?",
                "en": "Do you agree to the use of your contribution as described in the notice?",
                "es": "¿Autoriza el uso de su contribución según el aviso de participación?",
                "pt": "Autoriza a utilização da sua contribuição conforme o aviso de participação?",
                "ar": "هل توافق على استخدام مساهمتك وفق إشعار المشاركة؟"
              },
              "type": "single",
              "options": [
                {
                  "code": 1,
                  "label": {
                    "fr": "J’accepte",
                    "en": "I agree",
                    "es": "Acepto",
                    "pt": "Aceito",
                    "ar": "أوافق"
                  }
                },
                {
                  "code": 0,
                  "label": {
                    "fr": "Je ne participe pas — fin du questionnaire",
                    "en": "I do not wish to participate — end of questionnaire",
                    "es": "No deseo participar — fin del cuestionario",
                    "pt": "Não desejo participar — fim do questionário",
                    "ar": "لا أرغب في المشاركة — نهاية الاستبيان"
                  }
                }
              ],
              "required": true,
              "source": "Rapport méthodologique du 15 novembre 2026, pp. 31–32, QR01"
            },
            {
              "id": "QR01_Organisation",
              "label": {
                "fr": "Organisation",
                "en": "Organisation",
                "es": "Organización",
                "pt": "Organização",
                "ar": "المنظمة"
              },
              "type": "text",
              "options": [],
              "max_chars": 500,
              "source": "Rapport méthodologique du 15 novembre 2026, pp. 31–32, QR01",
              "gate": {
                "field": "QR01_Participation",
                "op": "in",
                "values": [
                  1
                ]
              }
            },
            {
              "id": "QR01_Type",
              "label": {
                "fr": "Type d’organisation",
                "en": "Type of organisation",
                "es": "Tipo de organización",
                "pt": "Tipo de organização",
                "ar": "نوع المنظمة"
              },
              "type": "single",
              "options": [
                {
                  "code": 1,
                  "label": {
                    "fr": "Communauté économique régionale (CER)",
                    "en": "Regional Economic Community (REC)",
                    "es": "Comunidad económica regional (CER)",
                    "pt": "Comunidade económica regional (CER)",
                    "ar": "جماعة اقتصادية إقليمية"
                  }
                },
                {
                  "code": 2,
                  "label": {
                    "fr": "Organisation internationale",
                    "en": "International organisation",
                    "es": "Organización internacional",
                    "pt": "Organização internacional",
                    "ar": "منظمة دولية"
                  }
                },
                {
                  "code": 97,
                  "label": {
                    "fr": "Autre",
                    "en": "Other",
                    "es": "Otra",
                    "pt": "Outra",
                    "ar": "أخرى"
                  }
                }
              ],
              "source": "Rapport méthodologique du 15 novembre 2026, pp. 31–32, QR01",
              "gate": {
                "field": "QR01_Participation",
                "op": "in",
                "values": [
                  1
                ]
              }
            },
            {
              "id": "QR01_Type_Autre",
              "label": {
                "fr": "Autre : précisez",
                "en": "Other: please specify",
                "es": "Otra : especifique",
                "pt": "Outra : especifique",
                "ar": "أخرى : يرجى التحديد"
              },
              "type": "text",
              "options": [],
              "max_chars": 500,
              "gate": {
                "all": [
                  {
                    "field": "QR01_Participation",
                    "op": "in",
                    "values": [
                      1
                    ]
                  },
                  {
                    "field": "QR01_Type",
                    "op": "in",
                    "values": [
                      97
                    ]
                  }
                ]
              },
              "source": "Rapport méthodologique du 15 novembre 2026, pp. 31–32, QR01"
            },
            {
              "id": "QR01_Fonction",
              "label": {
                "fr": "Fonction du service répondant, sans nom personnel",
                "en": "Function of the responding service, without a personal name",
                "es": "Función del servicio que responde, sin nombre personal",
                "pt": "Função do serviço que responde, sem nome pessoal",
                "ar": "وظيفة الإدارة المجيبة، دون اسم شخصي"
              },
              "type": "text",
              "options": [],
              "max_chars": 500,
              "source": "Rapport méthodologique du 15 novembre 2026, pp. 31–32, QR01",
              "gate": {
                "field": "QR01_Participation",
                "op": "in",
                "values": [
                  1
                ]
              }
            },
            {
              "id": "QR01_Statut",
              "label": {
                "fr": "Statut de la contribution",
                "en": "Contribution status",
                "es": "Carácter de la contribución",
                "pt": "Estatuto da contribuição",
                "ar": "صفة المساهمة"
              },
              "type": "single",
              "options": [
                {
                  "code": 1,
                  "label": {
                    "fr": "Réponse institutionnelle consolidée",
                    "en": "Consolidated institutional response",
                    "es": "Respuesta institucional consolidada",
                    "pt": "Resposta institucional consolidada",
                    "ar": "إجابة مؤسسية موحدة"
                  }
                },
                {
                  "code": 2,
                  "label": {
                    "fr": "Contribution d’un service, non consolidée au niveau de l’institution",
                    "en": "Service-level contribution, not consolidated at institution level",
                    "es": "Contribución de un servicio, no consolidada a nivel institucional",
                    "pt": "Contribuição de um serviço, não consolidada ao nível da instituição",
                    "ar": "مساهمة من إدارة، غير موحدة على مستوى المؤسسة"
                  }
                },
                {
                  "code": 3,
                  "label": {
                    "fr": "Contribution individuelle fondée sur ma pratique professionnelle",
                    "en": "Individual contribution based on my professional practice",
                    "es": "Contribución individual basada en mi práctica profesional",
                    "pt": "Contribuição individual baseada na minha prática profissional",
                    "ar": "مساهمة فردية تستند إلى ممارستي المهنية"
                  }
                }
              ],
              "source": "Rapport méthodologique du 15 novembre 2026, pp. 31–32, QR01",
              "gate": {
                "field": "QR01_Participation",
                "op": "in",
                "values": [
                  1
                ]
              }
            },
            {
              "id": "QR01_Champ",
              "label": {
                "fr": "Territoires ou institutions couverts par votre expérience, sans supposer la couverture de tous les membres",
                "en": "Territories or institutions covered by your experience, without assuming coverage of all members",
                "es": "Territorios o instituciones cubiertos por su experiencia, sin presuponer todos los miembros",
                "pt": "Territórios ou instituições abrangidos pela sua experiência, sem pressupor todos os membros",
                "ar": "المناطق أو المؤسسات المشمولة بخبرتكم، دون افتراض شمول جميع الأعضاء"
              },
              "type": "textarea",
              "options": [],
              "max_chars": 2500,
              "source": "Rapport méthodologique du 15 novembre 2026, pp. 31–32, QR01",
              "gate": {
                "field": "QR01_Participation",
                "op": "in",
                "values": [
                  1
                ]
              }
            },
            {
              "id": "QR01_Unite",
              "label": {
                "fr": "Service ou unité concernée",
                "en": "Service or unit concerned",
                "es": "Servicio o unidad correspondiente",
                "pt": "Serviço ou unidade em causa",
                "ar": "الإدارة أو الوحدة المعنية"
              },
              "type": "text",
              "options": [],
              "max_chars": 500,
              "source_extra": "Gestion de collecte — § 5.1 ; pas une question substantielle supplémentaire",
              "source": "Gestion de collecte — § 5.1 ; pas une question substantielle supplémentaire",
              "gate": {
                "field": "QR01_Participation",
                "op": "in",
                "values": [
                  1
                ]
              }
            },
            {
              "id": "QR01_Domaine",
              "label": {
                "fr": "Domaine ou secteur, facultatif",
                "en": "Domain or sector, optional",
                "es": "Ámbito o sector, opcional",
                "pt": "Domínio ou setor, facultativo",
                "ar": "المجال أو القطاع، اختياري"
              },
              "type": "text",
              "options": [],
              "max_chars": 500,
              "source_extra": "Gestion de collecte — § 5.1 ; pas une question substantielle supplémentaire",
              "source": "Gestion de collecte — § 5.1 ; pas une question substantielle supplémentaire",
              "gate": {
                "field": "QR01_Participation",
                "op": "in",
                "values": [
                  1
                ]
              }
            },
            {
              "id": "QR01_Langue_texte",
              "label": {
                "fr": "Langue principale de vos réponses libres",
                "en": "Main language of your free-text responses",
                "es": "Idioma principal de sus respuestas abiertas",
                "pt": "Língua principal das suas respostas abertas",
                "ar": "اللغة الأساسية لإجاباتك الحرة"
              },
              "type": "single",
              "options": [
                {
                  "code": "fr",
                  "label": {
                    "fr": "Français",
                    "en": "French",
                    "es": "Francés",
                    "pt": "Francês",
                    "ar": "الفرنسية"
                  }
                },
                {
                  "code": "en",
                  "label": {
                    "fr": "Anglais",
                    "en": "English",
                    "es": "Inglés",
                    "pt": "Inglês",
                    "ar": "الإنجليزية"
                  }
                },
                {
                  "code": "ar",
                  "label": {
                    "fr": "Arabe",
                    "en": "Arabic",
                    "es": "Árabe",
                    "pt": "Árabe",
                    "ar": "العربية"
                  }
                },
                {
                  "code": "pt",
                  "label": {
                    "fr": "Portugais",
                    "en": "Portuguese",
                    "es": "Portugués",
                    "pt": "Português",
                    "ar": "البرتغالية"
                  }
                },
                {
                  "code": "es",
                  "label": {
                    "fr": "Espagnol",
                    "en": "Spanish",
                    "es": "Español",
                    "pt": "Espanhol",
                    "ar": "الإسبانية"
                  }
                }
              ],
              "source_extra": "Gestion de collecte — § 5.1 ; pas une question substantielle supplémentaire",
              "source": "Gestion de collecte — § 5.1 ; pas une question substantielle supplémentaire",
              "gate": {
                "field": "QR01_Participation",
                "op": "in",
                "values": [
                  1
                ]
              }
            },
            {
              "id": "QR01_Origine_invitation",
              "label": {
                "fr": "Origine de l’invitation ou du lien",
                "en": "Origin of the invitation or link",
                "es": "Origen de la invitación o del enlace",
                "pt": "Origem do convite ou da ligação",
                "ar": "مصدر الدعوة أو الرابط"
              },
              "type": "single",
              "options": [
                {
                  "code": 1,
                  "label": {
                    "fr": "INS",
                    "en": "NSO",
                    "es": "ONE",
                    "pt": "INE",
                    "ar": "الجهاز الوطني للإحصاء"
                  }
                },
                {
                  "code": 2,
                  "label": {
                    "fr": "STATAFRIC ou consultant",
                    "en": "STATAFRIC or consultant",
                    "es": "STATAFRIC o consultor",
                    "pt": "STATAFRIC ou consultor",
                    "ar": "STATAFRIC أو الاستشاري"
                  }
                },
                {
                  "code": 3,
                  "label": {
                    "fr": "Autre relais",
                    "en": "Other forwarding organisation",
                    "es": "Otra entidad que transmitió la invitación",
                    "pt": "Outra entidade que encaminhou o convite",
                    "ar": "جهة أخرى أحالت الدعوة"
                  }
                },
                {
                  "code": 4,
                  "label": {
                    "fr": "Accès spontané",
                    "en": "Unsolicited access",
                    "es": "Acceso espontáneo",
                    "pt": "Acesso espontâneo",
                    "ar": "وصول تلقائي دون دعوة مباشرة"
                  }
                },
                {
                  "code": 98,
                  "label": {
                    "fr": "Ne sait pas",
                    "en": "Do not know",
                    "es": "No sabe",
                    "pt": "Não sabe",
                    "ar": "لا أعرف"
                  }
                }
              ],
              "source_extra": "Gestion de collecte — § 5.1 ; pas une question substantielle supplémentaire",
              "source": "Gestion de collecte — § 5.1 ; pas une question substantielle supplémentaire",
              "gate": {
                "field": "QR01_Participation",
                "op": "in",
                "values": [
                  1
                ]
              }
            },
            {
              "id": "QR01_INS_relais",
              "label": {
                "fr": "INS ou organisme relais, facultatif",
                "en": "Forwarding NSO or organisation, optional",
                "es": "ONE o entidad que transmitió la invitación, opcional",
                "pt": "INE ou entidade que encaminhou o convite, facultativo",
                "ar": "الجهاز الوطني للإحصاء أو الجهة المحيلة، اختياري"
              },
              "type": "text",
              "options": [],
              "max_chars": 500,
              "source_extra": "Gestion de collecte — § 5.1 ; pas une question substantielle supplémentaire",
              "source": "Gestion de collecte — § 5.1 ; pas une question substantielle supplémentaire",
              "gate": {
                "field": "QR01_Participation",
                "op": "in",
                "values": [
                  1
                ]
              }
            }
          ],
          "note": {
            "fr": "",
            "en": "",
            "es": "",
            "pt": "",
            "ar": ""
          },
          "gate": null,
          "theme": "Suivi et moyens",
          "source": "Rapport méthodologique du 15 novembre 2026, pp. 31–32, QR01"
        },
        {
          "id": "QR02",
          "title": {
            "fr": "Appuis effectivement réalisés pendant la période",
            "en": "Support actually provided during the period",
            "es": "Apoyos efectivamente realizados durante el período",
            "pt": "Apoios efetivamente realizados durante o período",
            "ar": "أوجه الدعم المنجزة فعليا خلال الفترة"
          },
          "fields": [
            {
              "id": "QR02_Appui",
              "label": {
                "fr": "Avez-vous réalisé ou suivi un appui lié à l’utilisation des statistiques dans les discours ?",
                "en": "Have you provided or monitored support related to the use of statistics in speeches?",
                "es": "¿Ha realizado o seguido un apoyo relacionado con el uso de estadísticas en discursos?",
                "pt": "Realizou ou acompanhou um apoio relacionado com a utilização de estatísticas em discursos?",
                "ar": "هل قدمتم أو تابعتم دعما يتعلق باستخدام الإحصاءات في الخطابات؟"
              },
              "type": "single",
              "options": [
                {
                  "code": 1,
                  "label": {
                    "fr": "Oui",
                    "en": "Yes",
                    "es": "Sí",
                    "pt": "Sim",
                    "ar": "نعم"
                  }
                },
                {
                  "code": 0,
                  "label": {
                    "fr": "Non",
                    "en": "No",
                    "es": "No",
                    "pt": "Não",
                    "ar": "لا"
                  }
                },
                {
                  "code": 98,
                  "label": {
                    "fr": "Ne sait pas",
                    "en": "Do not know",
                    "es": "No sabe",
                    "pt": "Não sabe",
                    "ar": "لا أعرف"
                  }
                },
                {
                  "code": 99,
                  "label": {
                    "fr": "Hors de votre champ d’activité",
                    "en": "Outside your area of activity",
                    "es": "Fuera de su ámbito de actividad",
                    "pt": "Fora do seu âmbito de atividade",
                    "ar": "خارج نطاق نشاطكم"
                  }
                }
              ],
              "source": "Rapport méthodologique du 15 novembre 2026, pp. 31–32, QR02",
              "gate": {
                "field": "QR01_Participation",
                "op": "in",
                "values": [
                  1
                ]
              }
            },
            {
              "id": "QR02_Activites",
              "label": {
                "fr": "Si oui, cocher les activités réellement connues",
                "en": "If yes, select the activities actually known",
                "es": "Si respondió sí, seleccione las actividades realmente conocidas",
                "pt": "Se sim, selecione as atividades efetivamente conhecidas",
                "ar": "إذا كانت الإجابة نعم، حدد الأنشطة المعروفة فعليا"
              },
              "type": "multi",
              "options": [
                {
                  "code": 1,
                  "label": {
                    "fr": "Guide ou modèle",
                    "en": "Guide or template",
                    "es": "Guía o modelo",
                    "pt": "Guia ou modelo",
                    "ar": "دليل أو نموذج"
                  }
                },
                {
                  "code": 2,
                  "label": {
                    "fr": "Formation ou accompagnement",
                    "en": "Training or support",
                    "es": "Formación o acompañamiento",
                    "pt": "Formação ou acompanhamento",
                    "ar": "تدريب أو مواكبة"
                  }
                },
                {
                  "code": 3,
                  "label": {
                    "fr": "Fourniture ou vérification de chiffres",
                    "en": "Providing or checking figures",
                    "es": "Suministro o comprobación de cifras",
                    "pt": "Fornecimento ou verificação de números",
                    "ar": "تقديم الأرقام أو التحقق منها"
                  }
                },
                {
                  "code": 4,
                  "label": {
                    "fr": "Appui à une procédure nationale",
                    "en": "Support for a national procedure",
                    "es": "Apoyo a un procedimiento nacional",
                    "pt": "Apoio a um procedimento nacional",
                    "ar": "دعم إجراء وطني"
                  }
                },
                {
                  "code": 5,
                  "label": {
                    "fr": "Diffusion ou partage d’expériences",
                    "en": "Dissemination or sharing experiences",
                    "es": "Difusión o intercambio de experiencias",
                    "pt": "Divulgação ou partilha de experiências",
                    "ar": "النشر أو تبادل الخبرات"
                  }
                },
                {
                  "code": 97,
                  "label": {
                    "fr": "Autre",
                    "en": "Other",
                    "es": "Otra",
                    "pt": "Outra",
                    "ar": "أخرى"
                  }
                }
              ],
              "gate": {
                "all": [
                  {
                    "field": "QR01_Participation",
                    "op": "in",
                    "values": [
                      1
                    ]
                  },
                  {
                    "field": "QR02_Appui",
                    "op": "in",
                    "values": [
                      1
                    ]
                  }
                ]
              },
              "source": "Rapport méthodologique du 15 novembre 2026, pp. 31–32, QR02"
            },
            {
              "id": "QR02_Activites_Autre",
              "label": {
                "fr": "Autre : précisez",
                "en": "Other: please specify",
                "es": "Otra : especifique",
                "pt": "Outra : especifique",
                "ar": "أخرى : يرجى التحديد"
              },
              "type": "text",
              "options": [],
              "max_chars": 500,
              "gate": {
                "all": [
                  {
                    "field": "QR01_Participation",
                    "op": "in",
                    "values": [
                      1
                    ]
                  },
                  {
                    "field": "QR02_Activites",
                    "op": "contains",
                    "values": [
                      97
                    ]
                  }
                ]
              },
              "source": "Rapport méthodologique du 15 novembre 2026, pp. 31–32, QR02"
            },
            {
              "id": "QR02_Cas",
              "label": {
                "fr": "Décrire un cas en deux phrases : période, type d’institution bénéficiaire et réalisation observable. Ajouter une référence publique si disponible.",
                "en": "Describe one case in two sentences: period, type of beneficiary institution and observable output. Add a public reference if available.",
                "es": "Describa un caso en dos frases : período, tipo de institución beneficiaria y resultado observable. Añada una referencia pública si existe.",
                "pt": "Descreva um caso em duas frases : período, tipo de instituição beneficiária e realização observável. Acrescente uma referência pública se disponível.",
                "ar": "صف حالة في جملتين : الفترة ونوع المؤسسة المستفيدة والإنجاز القابل للملاحظة. أضف مرجعا عاما إن توفر."
              },
              "type": "textarea",
              "options": [],
              "max_chars": 2500,
              "gate": {
                "all": [
                  {
                    "field": "QR01_Participation",
                    "op": "in",
                    "values": [
                      1
                    ]
                  },
                  {
                    "field": "QR02_Appui",
                    "op": "in",
                    "values": [
                      1
                    ]
                  }
                ]
              },
              "source": "Rapport méthodologique du 15 novembre 2026, pp. 31–32, QR02"
            }
          ],
          "note": {
            "fr": "Ne pas interpréter une participation à une formation comme la preuve d’une réforme appliquée.",
            "en": "Participation in training must not be interpreted as proof that a reform has been implemented.",
            "es": "Participar en una formación no demuestra que se haya aplicado una reforma.",
            "pt": "Participar numa formação não demonstra que uma reforma foi implementada.",
            "ar": "المشاركة في تدريب لا تثبت تطبيق إصلاح."
          },
          "gate": null,
          "theme": "Suivi et moyens",
          "source": "Rapport méthodologique du 15 novembre 2026, pp. 31–32, QR02"
        },
        {
          "id": "QR03",
          "title": {
            "fr": "Pratiques transférables et conditions d’adaptation",
            "en": "Transferable practices and conditions for adaptation",
            "es": "Prácticas transferibles y condiciones de adaptación",
            "pt": "Práticas transferíveis e condições de adaptação",
            "ar": "الممارسات القابلة للنقل وشروط تكييفها"
          },
          "fields": [
            {
              "id": "QR03_Absence",
              "label": {
                "fr": "Si vous ne proposez aucune pratique, indiquer la situation ; sinon laisser ce champ vide",
                "en": "If you are not proposing a practice, indicate the situation; otherwise leave this field blank",
                "es": "Si no propone una práctica, indique la situación; en caso contrario, deje este campo vacío",
                "pt": "Se não propõe uma prática, indique a situação; caso contrário, deixe este campo vazio",
                "ar": "إذا لم تقترح ممارسة، حدد الحالة؛ وإلا فاترك هذا الحقل فارغا"
              },
              "type": "single",
              "options": [
                {
                  "code": 0,
                  "label": {
                    "fr": "Aucune pratique documentée à proposer",
                    "en": "No documented practice to propose",
                    "es": "Ninguna práctica documentada que proponer",
                    "pt": "Nenhuma prática documentada a propor",
                    "ar": "لا توجد ممارسة موثقة لاقتراحها"
                  }
                },
                {
                  "code": 98,
                  "label": {
                    "fr": "Ne sait pas",
                    "en": "Do not know",
                    "es": "No sabe",
                    "pt": "Não sabe",
                    "ar": "لا أعرف"
                  }
                }
              ],
              "source": "Rapport méthodologique du 15 novembre 2026, pp. 31–32, QR03",
              "gate": {
                "field": "QR01_Participation",
                "op": "in",
                "values": [
                  1
                ]
              }
            },
            {
              "id": "QR03_Pratiques",
              "label": {
                "fr": "Au maximum deux pratiques : pratique ; source ou preuve ; contexte où elle a fonctionné ; condition nécessaire à son adaptation. Au plus 80 mots au total.",
                "en": "Up to two practices: practice; source or evidence; context in which it worked; condition required for adaptation. Up to 80 words in total.",
                "es": "Hasta dos prácticas : práctica; fuente o prueba; contexto en que funcionó; condición para adaptarla. Máximo de 80 palabras en total.",
                "pt": "Até duas práticas : prática; fonte ou prova; contexto em que funcionou; condição para adaptação. Máximo de 80 palavras no total.",
                "ar": "ممارستان كحد أقصى : الممارسة؛ المصدر أو الإثبات؛ السياق الذي نجحت فيه؛ شرط تكييفها. حتى 80 كلمة إجمالا."
              },
              "type": "textarea",
              "options": [],
              "max_chars": 2500,
              "max_words": 80,
              "gate": {
                "all": [
                  {
                    "field": "QR01_Participation",
                    "op": "in",
                    "values": [
                      1
                    ]
                  },
                  {
                    "field": "QR03_Absence",
                    "op": "not_in",
                    "values": [
                      0,
                      98
                    ]
                  }
                ]
              },
              "source": "Rapport méthodologique du 15 novembre 2026, pp. 31–32, QR03"
            }
          ],
          "note": {
            "fr": "Citer seulement des pratiques effectivement observées ou documentées. Un exemple plus ancien peut être cité avec sa date ; il ne constitue pas un appui réalisé pendant la période.",
            "en": "Cite only practices actually observed or documented. An older example may be cited with its date; it is not support provided during the reference period.",
            "es": "Cite solo prácticas realmente observadas o documentadas. Puede citar un ejemplo anterior con su fecha, sin presentarlo como apoyo realizado durante el período.",
            "pt": "Cite apenas práticas realmente observadas ou documentadas. Pode citar um exemplo anterior com a data, sem o apresentar como apoio realizado no período.",
            "ar": "اذكر فقط ممارسات شوهدت أو وثقت فعليا. يمكن ذكر مثال أقدم مع تاريخه، لكنه لا يعد دعما أنجز خلال الفترة."
          },
          "gate": null,
          "theme": "Suivi et moyens",
          "source": "Rapport méthodologique du 15 novembre 2026, pp. 31–32, QR03"
        },
        {
          "id": "QR03bis",
          "title": {
            "fr": "Normes et référentiels internationaux",
            "en": "International standards and frameworks",
            "es": "Normas y marcos internacionales",
            "pt": "Normas e referenciais internacionais",
            "ar": "المعايير والأطر المرجعية الدولية"
          },
          "fields": [
            {
              "id": "QR03bis_Presence",
              "label": {
                "fr": "Quels textes internationaux ou régionaux utilisez-vous ou recommandez-vous pour la qualité et la bonne utilisation des statistiques dans les discours et la décision ?",
                "en": "Which international or regional texts do you use or recommend for the quality and proper use of statistics in speeches and decision-making?",
                "es": "¿Qué textos internacionales o regionales utiliza o recomienda para la calidad y el buen uso de las estadísticas en discursos y decisiones?",
                "pt": "Que textos internacionais ou regionais utiliza ou recomenda para a qualidade e boa utilização das estatísticas nos discursos e na decisão?",
                "ar": "ما النصوص الدولية أو الإقليمية التي تستخدمونها أو توصي بها لضمان جودة الإحصاءات وحسن استخدامها في الخطابات واتخاذ القرار؟"
              },
              "type": "single",
              "options": [
                {
                  "code": 1,
                  "label": {
                    "fr": "Une ou plusieurs références à citer",
                    "en": "One or more references to cite",
                    "es": "Una o varias referencias que citar",
                    "pt": "Uma ou várias referências a citar",
                    "ar": "مرجع أو مراجع يمكن ذكرها"
                  }
                },
                {
                  "code": 0,
                  "label": {
                    "fr": "Aucune référence à proposer",
                    "en": "No reference to propose",
                    "es": "Ninguna referencia que proponer",
                    "pt": "Nenhuma referência a propor",
                    "ar": "لا يوجد مرجع لاقتراحه"
                  }
                },
                {
                  "code": 98,
                  "label": {
                    "fr": "Ne sait pas",
                    "en": "Do not know",
                    "es": "No sabe",
                    "pt": "Não sabe",
                    "ar": "لا أعرف"
                  }
                },
                {
                  "code": 99,
                  "label": {
                    "fr": "Hors de mon champ",
                    "en": "Outside my remit",
                    "es": "Fuera de mi ámbito",
                    "pt": "Fora do meu âmbito",
                    "ar": "خارج نطاق اختصاصي"
                  }
                }
              ],
              "source": "Rapport méthodologique, annexe 3, pp. 31–32",
              "gate": {
                "field": "QR01_Participation",
                "op": "in",
                "values": [
                  1
                ]
              }
            },
            {
              "id": "QR03bis_R1",
              "label": {
                "fr": "Référence 1 : titre exact ; organisme auteur ; année ou version ; lien public ou repère ; principe utile ; usage pour le discours ou la décision ; condition d’adaptation à un INS.",
                "en": "Reference 1: exact title; authoring organisation; year or version; public link or reference; useful principle; use for speeches or decisions; condition for adaptation to an NSO.",
                "es": "Referencia 1 : título exacto; organismo autor; año o versión; enlace público o referencia; principio útil; uso en discursos o decisiones; condición de adaptación a una ONE.",
                "pt": "Referência 1 : título exato; organismo autor; ano ou versão; ligação pública ou referência; princípio útil; uso em discursos ou decisões; condição de adaptação a um INE.",
                "ar": "المرجع 1 : العنوان الدقيق؛ الجهة المؤلفة؛ السنة أو النسخة؛ الرابط العام أو المرجع؛ المبدأ المفيد؛ استخدامه للخطاب أو القرار؛ شرط تكييفه لجهاز وطني للإحصاء."
              },
              "type": "textarea",
              "options": [],
              "max_chars": 2500,
              "gate": {
                "all": [
                  {
                    "field": "QR01_Participation",
                    "op": "in",
                    "values": [
                      1
                    ]
                  },
                  {
                    "field": "QR03bis_Presence",
                    "op": "in",
                    "values": [
                      1
                    ]
                  }
                ]
              },
              "source": "Rapport méthodologique, annexe 3, pp. 31–32"
            },
            {
              "id": "QR03bis_R1_Statut",
              "label": {
                "fr": "Statut de cette référence",
                "en": "Status of this reference",
                "es": "Situación de esta referencia",
                "pt": "Situação desta referência",
                "ar": "حالة هذا المرجع"
              },
              "type": "single",
              "options": [
                {
                  "code": 1,
                  "label": {
                    "fr": "Utilisée pendant la période",
                    "en": "Used during the period",
                    "es": "Utilizada durante el período",
                    "pt": "Utilizada durante o período",
                    "ar": "استخدم خلال الفترة"
                  }
                },
                {
                  "code": 2,
                  "label": {
                    "fr": "Recommandée, sans usage observé",
                    "en": "Recommended, without observed use",
                    "es": "Recomendada, sin uso observado",
                    "pt": "Recomendada, sem utilização observada",
                    "ar": "موصى به دون استخدام ملاحظ"
                  }
                },
                {
                  "code": 3,
                  "label": {
                    "fr": "Utilisée et recommandée",
                    "en": "Used and recommended",
                    "es": "Utilizada y recomendada",
                    "pt": "Utilizada e recomendada",
                    "ar": "مستخدم وموصى به"
                  }
                },
                {
                  "code": 98,
                  "label": {
                    "fr": "Ne sait pas",
                    "en": "Do not know",
                    "es": "No sabe",
                    "pt": "Não sabe",
                    "ar": "لا أعرف"
                  }
                }
              ],
              "gate": {
                "all": [
                  {
                    "field": "QR01_Participation",
                    "op": "in",
                    "values": [
                      1
                    ]
                  },
                  {
                    "field": "QR03bis_Presence",
                    "op": "in",
                    "values": [
                      1
                    ]
                  }
                ]
              },
              "source": "Rapport méthodologique, annexe 3, pp. 31–32"
            },
            {
              "id": "QR03bis_R1_Type",
              "label": {
                "fr": "Type de référence",
                "en": "Type of reference",
                "es": "Tipo de referencia",
                "pt": "Tipo de referência",
                "ar": "نوع المرجع"
              },
              "type": "single",
              "options": [
                {
                  "code": 1,
                  "label": {
                    "fr": "Texte normatif",
                    "en": "Normative text",
                    "es": "Texto normativo",
                    "pt": "Texto normativo",
                    "ar": "نص معياري"
                  }
                },
                {
                  "code": 2,
                  "label": {
                    "fr": "Guide ou manuel",
                    "en": "Guide or manual",
                    "es": "Guía o manual",
                    "pt": "Guia ou manual",
                    "ar": "دليل أو كتيب"
                  }
                },
                {
                  "code": 3,
                  "label": {
                    "fr": "Recommandation institutionnelle",
                    "en": "Institutional recommendation",
                    "es": "Recomendación institucional",
                    "pt": "Recomendação institucional",
                    "ar": "توصية مؤسسية"
                  }
                },
                {
                  "code": 97,
                  "label": {
                    "fr": "Autre",
                    "en": "Other",
                    "es": "Otra",
                    "pt": "Outra",
                    "ar": "أخرى"
                  }
                },
                {
                  "code": 98,
                  "label": {
                    "fr": "Ne sait pas",
                    "en": "Do not know",
                    "es": "No sabe",
                    "pt": "Não sabe",
                    "ar": "لا أعرف"
                  }
                }
              ],
              "gate": {
                "all": [
                  {
                    "field": "QR01_Participation",
                    "op": "in",
                    "values": [
                      1
                    ]
                  },
                  {
                    "field": "QR03bis_Presence",
                    "op": "in",
                    "values": [
                      1
                    ]
                  }
                ]
              },
              "source": "Rapport méthodologique, annexe 3, pp. 31–32"
            },
            {
              "id": "QR03bis_R2",
              "label": {
                "fr": "Référence 2 (facultative) : titre exact ; organisme auteur ; année ou version ; lien public ou repère ; principe utile ; usage pour le discours ou la décision ; condition d’adaptation à un INS.",
                "en": "Reference 2 (optional): exact title; authoring organisation; year or version; public link or reference; useful principle; use for speeches or decisions; condition for adaptation to an NSO.",
                "es": "Referencia 2 (opcional) : título exacto; organismo autor; año o versión; enlace público o referencia; principio útil; uso en discursos o decisiones; condición de adaptación a una ONE.",
                "pt": "Referência 2 (facultativa) : título exato; organismo autor; ano ou versão; ligação pública ou referência; princípio útil; uso em discursos ou decisões; condição de adaptação a um INE.",
                "ar": "المرجع 2 (اختياري) : العنوان الدقيق؛ الجهة المؤلفة؛ السنة أو النسخة؛ الرابط العام أو المرجع؛ المبدأ المفيد؛ استخدامه للخطاب أو القرار؛ شرط تكييفه لجهاز وطني للإحصاء."
              },
              "type": "textarea",
              "options": [],
              "max_chars": 2500,
              "gate": {
                "all": [
                  {
                    "field": "QR01_Participation",
                    "op": "in",
                    "values": [
                      1
                    ]
                  },
                  {
                    "field": "QR03bis_Presence",
                    "op": "in",
                    "values": [
                      1
                    ]
                  }
                ]
              },
              "source": "Rapport méthodologique, annexe 3, pp. 31–32"
            },
            {
              "id": "QR03bis_R2_Statut",
              "label": {
                "fr": "Statut de cette référence",
                "en": "Status of this reference",
                "es": "Situación de esta referencia",
                "pt": "Situação desta referência",
                "ar": "حالة هذا المرجع"
              },
              "type": "single",
              "options": [
                {
                  "code": 1,
                  "label": {
                    "fr": "Utilisée pendant la période",
                    "en": "Used during the period",
                    "es": "Utilizada durante el período",
                    "pt": "Utilizada durante o período",
                    "ar": "استخدم خلال الفترة"
                  }
                },
                {
                  "code": 2,
                  "label": {
                    "fr": "Recommandée, sans usage observé",
                    "en": "Recommended, without observed use",
                    "es": "Recomendada, sin uso observado",
                    "pt": "Recomendada, sem utilização observada",
                    "ar": "موصى به دون استخدام ملاحظ"
                  }
                },
                {
                  "code": 3,
                  "label": {
                    "fr": "Utilisée et recommandée",
                    "en": "Used and recommended",
                    "es": "Utilizada y recomendada",
                    "pt": "Utilizada e recomendada",
                    "ar": "مستخدم وموصى به"
                  }
                },
                {
                  "code": 98,
                  "label": {
                    "fr": "Ne sait pas",
                    "en": "Do not know",
                    "es": "No sabe",
                    "pt": "Não sabe",
                    "ar": "لا أعرف"
                  }
                }
              ],
              "gate": {
                "all": [
                  {
                    "field": "QR01_Participation",
                    "op": "in",
                    "values": [
                      1
                    ]
                  },
                  {
                    "field": "QR03bis_Presence",
                    "op": "in",
                    "values": [
                      1
                    ]
                  }
                ]
              },
              "source": "Rapport méthodologique, annexe 3, pp. 31–32"
            },
            {
              "id": "QR03bis_R2_Type",
              "label": {
                "fr": "Type de référence",
                "en": "Type of reference",
                "es": "Tipo de referencia",
                "pt": "Tipo de referência",
                "ar": "نوع المرجع"
              },
              "type": "single",
              "options": [
                {
                  "code": 1,
                  "label": {
                    "fr": "Texte normatif",
                    "en": "Normative text",
                    "es": "Texto normativo",
                    "pt": "Texto normativo",
                    "ar": "نص معياري"
                  }
                },
                {
                  "code": 2,
                  "label": {
                    "fr": "Guide ou manuel",
                    "en": "Guide or manual",
                    "es": "Guía o manual",
                    "pt": "Guia ou manual",
                    "ar": "دليل أو كتيب"
                  }
                },
                {
                  "code": 3,
                  "label": {
                    "fr": "Recommandation institutionnelle",
                    "en": "Institutional recommendation",
                    "es": "Recomendación institucional",
                    "pt": "Recomendação institucional",
                    "ar": "توصية مؤسسية"
                  }
                },
                {
                  "code": 97,
                  "label": {
                    "fr": "Autre",
                    "en": "Other",
                    "es": "Otra",
                    "pt": "Outra",
                    "ar": "أخرى"
                  }
                },
                {
                  "code": 98,
                  "label": {
                    "fr": "Ne sait pas",
                    "en": "Do not know",
                    "es": "No sabe",
                    "pt": "Não sabe",
                    "ar": "لا أعرف"
                  }
                }
              ],
              "gate": {
                "all": [
                  {
                    "field": "QR01_Participation",
                    "op": "in",
                    "values": [
                      1
                    ]
                  },
                  {
                    "field": "QR03bis_Presence",
                    "op": "in",
                    "values": [
                      1
                    ]
                  }
                ]
              },
              "source": "Rapport méthodologique, annexe 3, pp. 31–32"
            },
            {
              "id": "QR03bis_Application",
              "label": {
                "fr": "Préciser une application concrète ou une limite connue, au plus 60 mots",
                "en": "Specify a practical application or known limitation, up to 60 words",
                "es": "Indique una aplicación concreta o una limitación conocida, máximo 60 palabras",
                "pt": "Indique uma aplicação concreta ou uma limitação conhecida, máximo de 60 palavras",
                "ar": "اذكر تطبيقا عمليا أو قيدا معروفا، حتى 60 كلمة"
              },
              "type": "textarea",
              "options": [],
              "max_chars": 2500,
              "max_words": 60,
              "gate": {
                "all": [
                  {
                    "field": "QR01_Participation",
                    "op": "in",
                    "values": [
                      1
                    ]
                  },
                  {
                    "field": "QR03bis_Presence",
                    "op": "in",
                    "values": [
                      1
                    ]
                  }
                ]
              },
              "source": "Rapport méthodologique, annexe 3, pp. 31–32"
            }
          ],
          "note": {
            "fr": "Répondre à partir de votre pratique ; aucune recherche supplémentaire n’est demandée. Deux références au maximum. Citer un texte ne signifie ni qu’il s’applique juridiquement à tous les pays ni qu’il est mis en œuvre.",
            "en": "Answer from your experience; no additional research is requested. At most two references. Citing a text does not mean that it legally applies to all countries or has been implemented.",
            "es": "Responda desde su experiencia, sin investigación adicional. Máximo dos referencias. Citar un texto no significa que se aplique jurídicamente en todos los países ni que se haya implementado.",
            "pt": "Responda a partir da sua experiência, sem investigação adicional. Máximo de duas referências. Citar um texto não significa que se aplique juridicamente a todos os países nem que tenha sido implementado.",
            "ar": "أجب انطلاقا من خبرتك، دون بحث إضافي. مرجعان كحد أقصى. ذكر نص لا يعني سريانه قانونيا على جميع البلدان أو تطبيقه فعليا."
          },
          "gate": null,
          "theme": "Suivi et moyens",
          "source": "Rapport méthodologique, annexe 3, pp. 31–32"
        },
        {
          "id": "QR04",
          "title": {
            "fr": "Besoins prioritaires et contribution envisageable",
            "en": "Priority needs and possible contribution",
            "es": "Necesidades prioritarias y posible contribución",
            "pt": "Necessidades prioritárias e contribuição possível",
            "ar": "الاحتياجات ذات الأولوية والمساهمة الممكنة"
          },
          "fields": [
            {
              "id": "QR04_Besoins",
              "label": {
                "fr": "Retenir au plus trois besoins",
                "en": "Select up to three needs",
                "es": "Seleccione hasta tres necesidades",
                "pt": "Selecione até três necessidades",
                "ar": "اختر ثلاثة احتياجات كحد أقصى"
              },
              "type": "multi",
              "options": [
                {
                  "code": 1,
                  "label": {
                    "fr": "Procédures communes adaptables",
                    "en": "Adaptable common procedures",
                    "es": "Procedimientos comunes adaptables",
                    "pt": "Procedimentos comuns adaptáveis",
                    "ar": "إجراءات مشتركة قابلة للتكييف"
                  }
                },
                {
                  "code": 2,
                  "label": {
                    "fr": "Sources et fiches de chiffres",
                    "en": "Sources and fact sheets",
                    "es": "Fuentes y fichas de cifras",
                    "pt": "Fontes e fichas de números",
                    "ar": "مصادر وبطاقات أرقام"
                  }
                },
                {
                  "code": 3,
                  "label": {
                    "fr": "Contrôle et interprétation",
                    "en": "Checking and interpretation",
                    "es": "Control e interpretación",
                    "pt": "Controlo e interpretação",
                    "ar": "المراجعة والتفسير"
                  }
                },
                {
                  "code": 4,
                  "label": {
                    "fr": "Communication et formulation",
                    "en": "Communication and wording",
                    "es": "Comunicación y formulación",
                    "pt": "Comunicação e formulação",
                    "ar": "الاتصال والصياغة"
                  }
                },
                {
                  "code": 5,
                  "label": {
                    "fr": "Formation",
                    "en": "Training",
                    "es": "Formación",
                    "pt": "Formação",
                    "ar": "التدريب"
                  }
                },
                {
                  "code": 6,
                  "label": {
                    "fr": "Partage d’expériences",
                    "en": "Sharing experiences",
                    "es": "Intercambio de experiencias",
                    "pt": "Partilha de experiências",
                    "ar": "تبادل الخبرات"
                  }
                },
                {
                  "code": 7,
                  "label": {
                    "fr": "Adaptation linguistique",
                    "en": "Language adaptation",
                    "es": "Adaptación lingüística",
                    "pt": "Adaptação linguística",
                    "ar": "التكييف اللغوي"
                  }
                },
                {
                  "code": 97,
                  "label": {
                    "fr": "Autre",
                    "en": "Other",
                    "es": "Otra",
                    "pt": "Outra",
                    "ar": "أخرى"
                  }
                },
                {
                  "code": 0,
                  "label": {
                    "fr": "Aucun besoin identifié",
                    "en": "No identified need",
                    "es": "Ninguna necesidad identificada",
                    "pt": "Nenhuma necessidade identificada",
                    "ar": "لم يحدد أي احتياج"
                  }
                },
                {
                  "code": 98,
                  "label": {
                    "fr": "Ne sait pas",
                    "en": "Do not know",
                    "es": "No sabe",
                    "pt": "Não sabe",
                    "ar": "لا أعرف"
                  }
                }
              ],
              "max_choices": 3,
              "exclusive": [
                0,
                98
              ],
              "source": "Rapport méthodologique du 15 novembre 2026, pp. 31–32, QR04",
              "gate": {
                "field": "QR01_Participation",
                "op": "in",
                "values": [
                  1
                ]
              }
            },
            {
              "id": "QR04_Besoins_Autre",
              "label": {
                "fr": "Autre : précisez",
                "en": "Other: please specify",
                "es": "Otra : especifique",
                "pt": "Outra : especifique",
                "ar": "أخرى : يرجى التحديد"
              },
              "type": "text",
              "options": [],
              "max_chars": 500,
              "gate": {
                "all": [
                  {
                    "field": "QR01_Participation",
                    "op": "in",
                    "values": [
                      1
                    ]
                  },
                  {
                    "field": "QR04_Besoins",
                    "op": "contains",
                    "values": [
                      97
                    ]
                  }
                ]
              },
              "source": "Rapport méthodologique du 15 novembre 2026, pp. 31–32, QR04"
            },
            {
              "id": "QR04_Contribution",
              "label": {
                "fr": "Quelle contribution votre organisation pourrait-elle envisager, sous réserve de sa décision interne ? Un choix.",
                "en": "What contribution might your organisation consider, subject to its internal decision? Select one.",
                "es": "¿Qué contribución podría considerar su organización, previa decisión interna? Una opción.",
                "pt": "Que contribuição poderia considerar a sua organização, mediante decisão interna? Uma opção.",
                "ar": "ما المساهمة التي يمكن لمنظمتكم النظر فيها، رهنا بقرارها الداخلي؟ اختيار واحد."
              },
              "type": "single",
              "options": [
                {
                  "code": 1,
                  "label": {
                    "fr": "Relais de consultation",
                    "en": "Relaying the consultation",
                    "es": "Transmitir la consulta",
                    "pt": "Encaminhar a consulta",
                    "ar": "إحالة الاستشارة"
                  }
                },
                {
                  "code": 2,
                  "label": {
                    "fr": "Documents publics",
                    "en": "Public documents",
                    "es": "Documentos públicos",
                    "pt": "Documentos públicos",
                    "ar": "وثائق عامة"
                  }
                },
                {
                  "code": 3,
                  "label": {
                    "fr": "Commentaires techniques",
                    "en": "Technical comments",
                    "es": "Comentarios técnicos",
                    "pt": "Comentários técnicos",
                    "ar": "ملاحظات فنية"
                  }
                },
                {
                  "code": 4,
                  "label": {
                    "fr": "Mise en relation avec des utilisateurs volontaires",
                    "en": "Contact with volunteer users",
                    "es": "Contacto con usuarios voluntarios",
                    "pt": "Contacto com utilizadores voluntários",
                    "ar": "الربط بمستخدمين متطوعين"
                  }
                },
                {
                  "code": 5,
                  "label": {
                    "fr": "Aucune contribution envisagée à ce stade",
                    "en": "No contribution envisaged at this stage",
                    "es": "Ninguna contribución prevista por ahora",
                    "pt": "Nenhuma contribuição prevista nesta fase",
                    "ar": "لا مساهمة متوقعة في هذه المرحلة"
                  }
                },
                {
                  "code": 97,
                  "label": {
                    "fr": "Autre",
                    "en": "Other",
                    "es": "Otra",
                    "pt": "Outra",
                    "ar": "أخرى"
                  }
                },
                {
                  "code": 98,
                  "label": {
                    "fr": "Ne sait pas",
                    "en": "Do not know",
                    "es": "No sabe",
                    "pt": "Não sabe",
                    "ar": "لا أعرف"
                  }
                }
              ],
              "source": "Rapport méthodologique du 15 novembre 2026, pp. 31–32, QR04",
              "gate": {
                "field": "QR01_Participation",
                "op": "in",
                "values": [
                  1
                ]
              }
            },
            {
              "id": "QR04_Contribution_Autre",
              "label": {
                "fr": "Autre : précisez",
                "en": "Other: please specify",
                "es": "Otra : especifique",
                "pt": "Outra : especifique",
                "ar": "أخرى : يرجى التحديد"
              },
              "type": "text",
              "options": [],
              "max_chars": 500,
              "gate": {
                "all": [
                  {
                    "field": "QR01_Participation",
                    "op": "in",
                    "values": [
                      1
                    ]
                  },
                  {
                    "field": "QR04_Contribution",
                    "op": "in",
                    "values": [
                      97
                    ]
                  }
                ]
              },
              "source": "Rapport méthodologique du 15 novembre 2026, pp. 31–32, QR04"
            },
            {
              "id": "QR04_Contrainte",
              "label": {
                "fr": "Préciser une contrainte de moyens, de mandat ou de langue, si utile",
                "en": "Specify a resource, mandate or language constraint, if useful",
                "es": "Indique una restricción de recursos, mandato o idioma, si resulta útil",
                "pt": "Indique uma restrição de recursos, mandato ou língua, se útil",
                "ar": "اذكر قيدا يتعلق بالموارد أو الاختصاص أو اللغة، إن كان مفيدا"
              },
              "type": "textarea",
              "options": [],
              "max_chars": 2500,
              "source": "Rapport méthodologique du 15 novembre 2026, pp. 31–32, QR04",
              "gate": {
                "field": "QR01_Participation",
                "op": "in",
                "values": [
                  1
                ]
              }
            }
          ],
          "note": {
            "fr": "Cette réponse ne constitue pas un engagement de financement ou de personnel.",
            "en": "This response is not a commitment of funding or staff.",
            "es": "Esta respuesta no supone un compromiso de financiación ni de personal.",
            "pt": "Esta resposta não constitui compromisso de financiamento nem de pessoal.",
            "ar": "لا تمثل هذه الإجابة التزاما بالتمويل أو بتوفير موظفين."
          },
          "gate": null,
          "theme": "Suivi et moyens",
          "source": "Rapport méthodologique du 15 novembre 2026, pp. 31–32, QR04"
        },
        {
          "id": "QR04bis",
          "title": {
            "fr": "Recommandations des organisations internationales",
            "en": "Recommendations from international organisations",
            "es": "Recomendaciones de organizaciones internacionales",
            "pt": "Recomendações de organizações internacionais",
            "ar": "توصيات المنظمات الدولية"
          },
          "fields": [
            {
              "id": "QR04bis_Presence",
              "label": {
                "fr": "Quelles recommandations d’organisations internationales devraient être prises en compte dans le document stratégique ?",
                "en": "Which recommendations from international organisations should be considered in the strategic document?",
                "es": "¿Qué recomendaciones de organizaciones internacionales deberían incorporarse al documento estratégico?",
                "pt": "Que recomendações de organizações internacionais devem ser consideradas no documento estratégico?",
                "ar": "ما توصيات المنظمات الدولية التي ينبغي أخذها في الاعتبار في الوثيقة الاستراتيجية؟"
              },
              "type": "single",
              "options": [
                {
                  "code": 1,
                  "label": {
                    "fr": "Une ou plusieurs recommandations à proposer",
                    "en": "One or more recommendations to propose",
                    "es": "Una o varias recomendaciones que proponer",
                    "pt": "Uma ou várias recomendações a propor",
                    "ar": "توصية أو توصيات يمكن اقتراحها"
                  }
                },
                {
                  "code": 0,
                  "label": {
                    "fr": "Aucune",
                    "en": "None",
                    "es": "Ninguna",
                    "pt": "Nenhuma",
                    "ar": "لا توجد"
                  }
                },
                {
                  "code": 98,
                  "label": {
                    "fr": "Ne sait pas",
                    "en": "Do not know",
                    "es": "No sabe",
                    "pt": "Não sabe",
                    "ar": "لا أعرف"
                  }
                },
                {
                  "code": 99,
                  "label": {
                    "fr": "Hors de mon champ",
                    "en": "Outside my remit",
                    "es": "Fuera de mi ámbito",
                    "pt": "Fora do meu âmbito",
                    "ar": "خارج نطاق اختصاصي"
                  }
                }
              ],
              "source": "Rapport méthodologique, annexe 3, p. 32",
              "gate": {
                "field": "QR01_Participation",
                "op": "in",
                "values": [
                  1
                ]
              }
            },
            {
              "id": "QR04bis_R1",
              "label": {
                "fr": "Recommandation 1 : organisation d’origine ; document, date et lien ; action ; acteur ; niveau national, régional ou continental ; difficulté ; moyens et conditions ; preuve de réalisation.",
                "en": "Recommendation 1: originating organisation; document, date and link; action; responsible actor; national, regional or continental level; difficulty; resources and conditions; evidence of implementation.",
                "es": "Recomendación 1 : organización de origen; documento, fecha y enlace; acción; actor responsable; nivel nacional, regional o continental; dificultad; recursos y condiciones; prueba de realización.",
                "pt": "Recomendação 1 : organização de origem; documento, data e ligação; ação; ator responsável; nível nacional, regional ou continental; dificuldade; recursos e condições; prova de realização.",
                "ar": "التوصية 1 : المنظمة الأصلية؛ الوثيقة والتاريخ والرابط؛ الإجراء؛ الجهة المسؤولة؛ المستوى الوطني أو الإقليمي أو القاري؛ الصعوبة؛ الموارد والشروط؛ إثبات التنفيذ."
              },
              "type": "textarea",
              "options": [],
              "max_chars": 2500,
              "gate": {
                "all": [
                  {
                    "field": "QR01_Participation",
                    "op": "in",
                    "values": [
                      1
                    ]
                  },
                  {
                    "field": "QR04bis_Presence",
                    "op": "in",
                    "values": [
                      1
                    ]
                  }
                ]
              },
              "source": "Rapport méthodologique, annexe 3, p. 32"
            },
            {
              "id": "QR04bis_R1_Nature",
              "label": {
                "fr": "Nature de cette recommandation",
                "en": "Nature of this recommendation",
                "es": "Naturaleza de esta recomendación",
                "pt": "Natureza desta recomendação",
                "ar": "طبيعة هذه التوصية"
              },
              "type": "single",
              "options": [
                {
                  "code": 1,
                  "label": {
                    "fr": "Recommandation institutionnelle publiée",
                    "en": "Published institutional recommendation",
                    "es": "Recomendación institucional publicada",
                    "pt": "Recomendação institucional publicada",
                    "ar": "توصية مؤسسية منشورة"
                  }
                },
                {
                  "code": 2,
                  "label": {
                    "fr": "Recommandation institutionnelle non publiée, partage autorisé",
                    "en": "Unpublished institutional recommendation, authorised for sharing",
                    "es": "Recomendación institucional no publicada, autorizada para compartir",
                    "pt": "Recomendação institucional não publicada, com partilha autorizada",
                    "ar": "توصية مؤسسية غير منشورة ومصرح بمشاركتها"
                  }
                },
                {
                  "code": 3,
                  "label": {
                    "fr": "Proposition professionnelle personnelle",
                    "en": "Personal professional proposal",
                    "es": "Propuesta profesional personal",
                    "pt": "Proposta profissional pessoal",
                    "ar": "اقتراح مهني شخصي"
                  }
                },
                {
                  "code": 98,
                  "label": {
                    "fr": "À préciser",
                    "en": "To be clarified",
                    "es": "Por aclarar",
                    "pt": "A esclarecer",
                    "ar": "بحاجة إلى توضيح"
                  }
                }
              ],
              "gate": {
                "all": [
                  {
                    "field": "QR01_Participation",
                    "op": "in",
                    "values": [
                      1
                    ]
                  },
                  {
                    "field": "QR04bis_Presence",
                    "op": "in",
                    "values": [
                      1
                    ]
                  }
                ]
              },
              "source": "Rapport méthodologique, annexe 3, p. 32"
            },
            {
              "id": "QR04bis_R2",
              "label": {
                "fr": "Recommandation 2 (facultative) : organisation d’origine ; document, date et lien ; action ; acteur ; niveau national, régional ou continental ; difficulté ; moyens et conditions ; preuve de réalisation.",
                "en": "Recommendation 2 (optional): originating organisation; document, date and link; action; responsible actor; national, regional or continental level; difficulty; resources and conditions; evidence of implementation.",
                "es": "Recomendación 2 (opcional) : organización de origen; documento, fecha y enlace; acción; actor responsable; nivel nacional, regional o continental; dificultad; recursos y condiciones; prueba de realización.",
                "pt": "Recomendação 2 (facultativa) : organização de origem; documento, data e ligação; ação; ator responsável; nível nacional, regional ou continental; dificuldade; recursos e condições; prova de realização.",
                "ar": "التوصية 2 (اختيارية) : المنظمة الأصلية؛ الوثيقة والتاريخ والرابط؛ الإجراء؛ الجهة المسؤولة؛ المستوى الوطني أو الإقليمي أو القاري؛ الصعوبة؛ الموارد والشروط؛ إثبات التنفيذ."
              },
              "type": "textarea",
              "options": [],
              "max_chars": 2500,
              "gate": {
                "all": [
                  {
                    "field": "QR01_Participation",
                    "op": "in",
                    "values": [
                      1
                    ]
                  },
                  {
                    "field": "QR04bis_Presence",
                    "op": "in",
                    "values": [
                      1
                    ]
                  }
                ]
              },
              "source": "Rapport méthodologique, annexe 3, p. 32"
            },
            {
              "id": "QR04bis_R2_Nature",
              "label": {
                "fr": "Nature de cette recommandation",
                "en": "Nature of this recommendation",
                "es": "Naturaleza de esta recomendación",
                "pt": "Natureza desta recomendação",
                "ar": "طبيعة هذه التوصية"
              },
              "type": "single",
              "options": [
                {
                  "code": 1,
                  "label": {
                    "fr": "Recommandation institutionnelle publiée",
                    "en": "Published institutional recommendation",
                    "es": "Recomendación institucional publicada",
                    "pt": "Recomendação institucional publicada",
                    "ar": "توصية مؤسسية منشورة"
                  }
                },
                {
                  "code": 2,
                  "label": {
                    "fr": "Recommandation institutionnelle non publiée, partage autorisé",
                    "en": "Unpublished institutional recommendation, authorised for sharing",
                    "es": "Recomendación institucional no publicada, autorizada para compartir",
                    "pt": "Recomendação institucional não publicada, com partilha autorizada",
                    "ar": "توصية مؤسسية غير منشورة ومصرح بمشاركتها"
                  }
                },
                {
                  "code": 3,
                  "label": {
                    "fr": "Proposition professionnelle personnelle",
                    "en": "Personal professional proposal",
                    "es": "Propuesta profesional personal",
                    "pt": "Proposta profissional pessoal",
                    "ar": "اقتراح مهني شخصي"
                  }
                },
                {
                  "code": 98,
                  "label": {
                    "fr": "À préciser",
                    "en": "To be clarified",
                    "es": "Por aclarar",
                    "pt": "A esclarecer",
                    "ar": "بحاجة إلى توضيح"
                  }
                }
              ],
              "gate": {
                "all": [
                  {
                    "field": "QR01_Participation",
                    "op": "in",
                    "values": [
                      1
                    ]
                  },
                  {
                    "field": "QR04bis_Presence",
                    "op": "in",
                    "values": [
                      1
                    ]
                  }
                ]
              },
              "source": "Rapport méthodologique, annexe 3, p. 32"
            },
            {
              "id": "QR04bis_Priorite",
              "label": {
                "fr": "Quelle recommandation est prioritaire et pourquoi ? Au plus 40 mots.",
                "en": "Which recommendation takes priority and why? Up to 40 words.",
                "es": "¿Qué recomendación es prioritaria y por qué? Máximo 40 palabras.",
                "pt": "Que recomendação é prioritária e porquê? Máximo de 40 palavras.",
                "ar": "أي توصية لها الأولوية ولماذا؟ حتى 40 كلمة."
              },
              "type": "textarea",
              "options": [],
              "max_chars": 2500,
              "max_words": 40,
              "gate": {
                "all": [
                  {
                    "field": "QR01_Participation",
                    "op": "in",
                    "values": [
                      1
                    ]
                  },
                  {
                    "field": "QR04bis_Presence",
                    "op": "in",
                    "values": [
                      1
                    ]
                  }
                ]
              },
              "source": "Rapport méthodologique, annexe 3, p. 32"
            }
          ],
          "note": {
            "fr": "Deux recommandations au maximum. Distinguer la recommandation institutionnelle documentée de la proposition personnelle. Ces propositions ne constituent ni un engagement de financement ni une décision d’adoption.",
            "en": "At most two recommendations. Distinguish a documented institutional recommendation from a personal proposal. These proposals are neither funding commitments nor adoption decisions.",
            "es": "Máximo dos recomendaciones. Distinga una recomendación institucional documentada de una propuesta personal. No constituyen compromisos de financiación ni decisiones de adopción.",
            "pt": "Máximo de duas recomendações. Distinga uma recomendação institucional documentada de uma proposta pessoal. Não constituem compromissos de financiamento nem decisões de adoção.",
            "ar": "توصيتان كحد أقصى. ميز التوصية المؤسسية الموثقة عن الاقتراح الشخصي. لا تشكل هذه المقترحات التزامات تمويل أو قرارات اعتماد."
          },
          "gate": null,
          "theme": "Suivi et moyens",
          "source": "Rapport méthodologique, annexe 3, p. 32"
        },
        {
          "id": "QR05",
          "title": {
            "fr": "Attribution et suite éventuelle",
            "en": "Attribution and possible follow-up",
            "es": "Atribución y posible seguimiento",
            "pt": "Atribuição e possível seguimento",
            "ar": "الإسناد والمتابعة المحتملة"
          },
          "fields": [
            {
              "id": "QR05_Attribution",
              "label": {
                "fr": "Réutilisation autorisée",
                "en": "Authorised reuse",
                "es": "Reutilización autorizada",
                "pt": "Reutilização autorizada",
                "ar": "إعادة الاستخدام المصرح بها"
              },
              "type": "single",
              "options": [
                {
                  "code": 0,
                  "label": {
                    "fr": "Synthèse anonymisée seulement, sans attribution identifiable",
                    "en": "Anonymised summary only, with no identifiable attribution",
                    "es": "Solo síntesis anonimizada, sin atribución identificable",
                    "pt": "Apenas síntese anonimizada, sem atribuição identificável",
                    "ar": "ملخص دون تحديد الهوية فقط، دون إسناد كاشف للهوية"
                  }
                },
                {
                  "code": 1,
                  "label": {
                    "fr": "Attribution à l’institution, sans citation textuelle identifiable",
                    "en": "Attribution to the institution, without an identifiable quotation",
                    "es": "Atribución a la institución, sin cita textual identificable",
                    "pt": "Atribuição à instituição, sem citação textual identificável",
                    "ar": "الإسناد إلى المؤسسة دون اقتباس نصي كاشف للهوية"
                  }
                },
                {
                  "code": 2,
                  "label": {
                    "fr": "Attribution à l’institution et citation textuelle",
                    "en": "Attribution to the institution and direct quotation",
                    "es": "Atribución a la institución y cita textual",
                    "pt": "Atribuição à instituição e citação textual",
                    "ar": "الإسناد إلى المؤسسة والاقتباس النصي"
                  }
                },
                {
                  "code": 3,
                  "label": {
                    "fr": "Me soumettre le passage avant toute attribution ou citation identifiable",
                    "en": "Submit the passage to me before any identifiable attribution or quotation",
                    "es": "Enviarme el pasaje antes de cualquier atribución o cita identificable",
                    "pt": "Enviar-me o trecho antes de qualquer atribuição ou citação identificável",
                    "ar": "اعرض علي المقطع قبل أي إسناد أو اقتباس كاشف للهوية"
                  }
                }
              ],
              "attribution": true,
              "source": "Rapport méthodologique du 15 novembre 2026, pp. 31–32, QR05",
              "gate": {
                "field": "QR01_Participation",
                "op": "in",
                "values": [
                  1
                ]
              }
            },
            {
              "id": "QR05_Contact_fonction",
              "label": {
                "fr": "Contact facultatif : fonction ou service à recontacter",
                "en": "Optional contact: function or service to contact",
                "es": "Contacto opcional : función o servicio al que contactar",
                "pt": "Contacto facultativo : função ou serviço a contactar",
                "ar": "جهة اتصال اختيارية : الوظيفة أو الإدارة التي يمكن التواصل معها"
              },
              "type": "text",
              "options": [],
              "max_chars": 500,
              "private": true,
              "source": "Rapport méthodologique du 15 novembre 2026, pp. 31–32, QR05",
              "gate": {
                "field": "QR01_Participation",
                "op": "in",
                "values": [
                  1
                ]
              }
            },
            {
              "id": "QR05_Contact_courriel",
              "label": {
                "fr": "Courriel institutionnel ou générique, facultatif",
                "en": "Institutional or generic email address, optional",
                "es": "Correo institucional o genérico, opcional",
                "pt": "Correio institucional ou genérico, facultativo",
                "ar": "بريد مؤسسي أو عام، اختياري"
              },
              "type": "email",
              "options": [],
              "max_chars": 254,
              "private": true,
              "source": "Rapport méthodologique du 15 novembre 2026, pp. 31–32, QR05",
              "gate": {
                "field": "QR01_Participation",
                "op": "in",
                "values": [
                  1
                ]
              }
            },
            {
              "id": "QR05_Contact_relais",
              "label": {
                "fr": "Préférer un relais par STATAFRIC, facultatif",
                "en": "Prefer contact through STATAFRIC, optional",
                "es": "Preferir contacto a través de STATAFRIC, opcional",
                "pt": "Preferir contacto através do STATAFRIC, facultativo",
                "ar": "تفضيل التواصل عبر STATAFRIC، اختياري"
              },
              "type": "single",
              "options": [
                {
                  "code": 1,
                  "label": {
                    "fr": "Passer par STATAFRIC",
                    "en": "Contact through STATAFRIC",
                    "es": "Contacto a través de STATAFRIC",
                    "pt": "Contacto através do STATAFRIC",
                    "ar": "التواصل عبر STATAFRIC"
                  }
                }
              ],
              "private": true,
              "source": "Rapport méthodologique du 15 novembre 2026, pp. 31–32, QR05",
              "gate": {
                "field": "QR01_Participation",
                "op": "in",
                "values": [
                  1
                ]
              }
            },
            {
              "id": "QR05_Nouveaute",
              "label": {
                "fr": "Pratique nouvelle après le 31 octobre, facultatif : description courte et date",
                "en": "New practice after 31 October, optional: brief description and date",
                "es": "Práctica nueva después del 31 de octubre, opcional : descripción breve y fecha",
                "pt": "Prática nova após 31 de outubro, facultativo : descrição breve e data",
                "ar": "ممارسة جديدة بعد 31 أكتوبر، اختياري : وصف موجز وتاريخ"
              },
              "type": "textarea",
              "options": [],
              "max_chars": 2500,
              "source": "Rapport méthodologique du 15 novembre 2026, pp. 31–32, QR05",
              "gate": {
                "field": "QR01_Participation",
                "op": "in",
                "values": [
                  1
                ]
              }
            }
          ],
          "note": {
            "fr": "Les choix 1 et 2 supposent l’autorisation correspondante. Une contribution individuelle sans cette autorisation reste anonymisée. Sans choix explicite, la synthèse anonymisée (code 0) s’applique. L’absence de retour ne vaut jamais accord de citation.",
            "en": "Options 1 and 2 require the corresponding authorisation. An individual contribution without that authorisation remains anonymised. Without an explicit choice, an anonymised summary (code 0) applies. Silence never constitutes permission to quote.",
            "es": "Las opciones 1 y 2 requieren la autorización correspondiente. Una contribución individual sin esa autorización permanece anonimizada. Sin selección explícita, se aplica la síntesis anonimizada (código 0). El silencio nunca autoriza una cita.",
            "pt": "As opções 1 e 2 exigem a autorização correspondente. Uma contribuição individual sem essa autorização mantém-se anonimizada. Sem escolha explícita, aplica-se a síntese anonimizada (código 0). O silêncio nunca autoriza uma citação.",
            "ar": "يتطلب الخياران 1 و2 التصريح المناسب. تبقى المساهمة الفردية دون هذا التصريح مجهولة الهوية. عند غياب اختيار صريح، يطبق الملخص دون تحديد الهوية (الرمز 0). الصمت لا يعني أبدا الإذن بالاقتباس."
          },
          "gate": null,
          "theme": "Suivi et moyens",
          "source": "Rapport méthodologique du 15 novembre 2026, pp. 31–32, QR05"
        }
      ],
      "consent": "QR01_Participation",
      "attribution": "QR05_Attribution",
      "institution": "QR01_Organisation",
      "country": "QR01_Champ",
      "status": "QR01_Statut",
      "version": "1.2 — 2026-11-15",
      "unit": "QR01_Unite",
      "sector": "QR01_Domaine",
      "response_language": "QR01_Langue_texte",
      "origin": "QR01_Origine_invitation",
      "relay": "QR01_INS_relais",
      "profile": "CER/OI",
      "priority_prefix": null
    },
    "abrege": {
      "title": {
        "fr": "Réponse abrégée",
        "en": "Short response",
        "es": "Respuesta abreviada",
        "pt": "Resposta abreviada",
        "ar": "إجابة مختصرة"
      },
      "duration": {
        "fr": "5 à 10 minutes",
        "en": "5 to 10 minutes",
        "es": "De 5 a 10 minutos",
        "pt": "5 a 10 minutos",
        "ar": "من 5 إلى 10 دقائق"
      },
      "sections": [
        {
          "id": "RA01",
          "title": {
            "fr": "Participation et périmètre",
            "en": "Participation and scope",
            "es": "Participación y ámbito",
            "pt": "Participação e âmbito",
            "ar": "المشاركة والنطاق"
          },
          "fields": [
            {
              "id": "RA01_Participation",
              "label": {
                "fr": "Acceptez-vous l’utilisation de votre contribution selon la notice ?",
                "en": "Do you agree to the use of your contribution as described in the notice?",
                "es": "¿Autoriza el uso de su contribución según el aviso de participación?",
                "pt": "Autoriza a utilização da sua contribuição conforme o aviso de participação?",
                "ar": "هل توافق على استخدام مساهمتك وفق إشعار المشاركة؟"
              },
              "type": "single",
              "options": [
                {
                  "code": 1,
                  "label": {
                    "fr": "J’accepte",
                    "en": "I agree",
                    "es": "Acepto",
                    "pt": "Aceito",
                    "ar": "أوافق"
                  }
                },
                {
                  "code": 0,
                  "label": {
                    "fr": "Je ne participe pas — fin du questionnaire",
                    "en": "I do not wish to participate — end of questionnaire",
                    "es": "No deseo participar — fin del cuestionario",
                    "pt": "Não desejo participar — fim do questionário",
                    "ar": "لا أرغب في المشاركة — نهاية الاستبيان"
                  }
                }
              ],
              "required": true,
              "source": "Rapport méthodologique du 15 novembre 2026, pp. 33–33, RA01"
            },
            {
              "id": "RA01_Pays",
              "label": {
                "fr": "Pays",
                "en": "Country",
                "es": "País",
                "pt": "País",
                "ar": "البلد"
              },
              "type": "text",
              "options": [],
              "max_chars": 500,
              "source": "Rapport méthodologique du 15 novembre 2026, pp. 33–33, RA01",
              "gate": {
                "field": "RA01_Participation",
                "op": "in",
                "values": [
                  1
                ]
              }
            },
            {
              "id": "RA01_Institution",
              "label": {
                "fr": "Institution ou type d’institution",
                "en": "Institution or type of institution",
                "es": "Institución o tipo de institución",
                "pt": "Instituição ou tipo de instituição",
                "ar": "المؤسسة أو نوع المؤسسة"
              },
              "type": "text",
              "options": [],
              "max_chars": 500,
              "source": "Rapport méthodologique du 15 novembre 2026, pp. 33–33, RA01",
              "gate": {
                "field": "RA01_Participation",
                "op": "in",
                "values": [
                  1
                ]
              }
            },
            {
              "id": "RA01_Fonction",
              "label": {
                "fr": "Fonction",
                "en": "Function",
                "es": "Función",
                "pt": "Função",
                "ar": "الوظيفة"
              },
              "type": "text",
              "options": [],
              "max_chars": 500,
              "source": "Rapport méthodologique du 15 novembre 2026, pp. 33–33, RA01",
              "gate": {
                "field": "RA01_Participation",
                "op": "in",
                "values": [
                  1
                ]
              }
            },
            {
              "id": "RA01_Profil",
              "label": {
                "fr": "Profil de la contribution",
                "en": "Contribution profile",
                "es": "Perfil de la contribución",
                "pt": "Perfil da contribuição",
                "ar": "فئة المساهمة"
              },
              "type": "single",
              "options": [
                {
                  "code": 1,
                  "label": {
                    "fr": "INS",
                    "en": "NSO",
                    "es": "ONE",
                    "pt": "INE",
                    "ar": "الجهاز الوطني للإحصاء"
                  }
                },
                {
                  "code": 2,
                  "label": {
                    "fr": "Autre producteur de statistiques",
                    "en": "Other producer of statistics",
                    "es": "Otro productor de estadísticas",
                    "pt": "Outro produtor de estatísticas",
                    "ar": "جهة أخرى منتجة للإحصاءات"
                  }
                },
                {
                  "code": 3,
                  "label": {
                    "fr": "Équipe préparant les discours",
                    "en": "Speech-preparation team",
                    "es": "Equipo que prepara discursos",
                    "pt": "Equipa que prepara discursos",
                    "ar": "فريق إعداد الخطابات"
                  }
                },
                {
                  "code": 4,
                  "label": {
                    "fr": "CER ou organisation internationale",
                    "en": "REC or international organisation",
                    "es": "CER u organización internacional",
                    "pt": "CER ou organização internacional",
                    "ar": "جماعة اقتصادية إقليمية أو منظمة دولية"
                  }
                },
                {
                  "code": 97,
                  "label": {
                    "fr": "Autre",
                    "en": "Other",
                    "es": "Otra",
                    "pt": "Outra",
                    "ar": "أخرى"
                  }
                }
              ],
              "source": "Rapport méthodologique du 15 novembre 2026, pp. 33–33, RA01",
              "gate": {
                "field": "RA01_Participation",
                "op": "in",
                "values": [
                  1
                ]
              }
            },
            {
              "id": "RA01_Profil_Autre",
              "label": {
                "fr": "Autre : précisez",
                "en": "Other: please specify",
                "es": "Otra : especifique",
                "pt": "Outra : especifique",
                "ar": "أخرى : يرجى التحديد"
              },
              "type": "text",
              "options": [],
              "max_chars": 500,
              "gate": {
                "all": [
                  {
                    "field": "RA01_Participation",
                    "op": "in",
                    "values": [
                      1
                    ]
                  },
                  {
                    "field": "RA01_Profil",
                    "op": "in",
                    "values": [
                      97
                    ]
                  }
                ]
              },
              "source": "Rapport méthodologique du 15 novembre 2026, pp. 33–33, RA01"
            },
            {
              "id": "RA01_Statut",
              "label": {
                "fr": "Réponse institutionnelle, de service ou individuelle",
                "en": "Institutional, service-level or individual response",
                "es": "Respuesta institucional, de servicio o individual",
                "pt": "Resposta institucional, de serviço ou individual",
                "ar": "إجابة مؤسسية أو من إدارة أو فردية"
              },
              "type": "single",
              "options": [
                {
                  "code": 1,
                  "label": {
                    "fr": "Réponse institutionnelle consolidée",
                    "en": "Consolidated institutional response",
                    "es": "Respuesta institucional consolidada",
                    "pt": "Resposta institucional consolidada",
                    "ar": "إجابة مؤسسية موحدة"
                  }
                },
                {
                  "code": 2,
                  "label": {
                    "fr": "Contribution d’un service, non consolidée au niveau de l’institution",
                    "en": "Service-level contribution, not consolidated at institution level",
                    "es": "Contribución de un servicio, no consolidada a nivel institucional",
                    "pt": "Contribuição de um serviço, não consolidada ao nível da instituição",
                    "ar": "مساهمة من إدارة، غير موحدة على مستوى المؤسسة"
                  }
                },
                {
                  "code": 3,
                  "label": {
                    "fr": "Contribution individuelle fondée sur ma pratique professionnelle",
                    "en": "Individual contribution based on my professional practice",
                    "es": "Contribución individual basada en mi práctica profesional",
                    "pt": "Contribuição individual baseada na minha prática profissional",
                    "ar": "مساهمة فردية تستند إلى ممارستي المهنية"
                  }
                }
              ],
              "source": "Rapport méthodologique du 15 novembre 2026, pp. 33–33, RA01",
              "gate": {
                "field": "RA01_Participation",
                "op": "in",
                "values": [
                  1
                ]
              }
            },
            {
              "id": "RA01_Unite",
              "label": {
                "fr": "Service ou unité concernée",
                "en": "Service or unit concerned",
                "es": "Servicio o unidad correspondiente",
                "pt": "Serviço ou unidade em causa",
                "ar": "الإدارة أو الوحدة المعنية"
              },
              "type": "text",
              "options": [],
              "max_chars": 500,
              "source_extra": "Gestion de collecte — § 5.1 ; pas une question substantielle supplémentaire",
              "source": "Gestion de collecte — § 5.1 ; pas une question substantielle supplémentaire",
              "gate": {
                "field": "RA01_Participation",
                "op": "in",
                "values": [
                  1
                ]
              }
            },
            {
              "id": "RA01_Domaine",
              "label": {
                "fr": "Domaine ou secteur, facultatif",
                "en": "Domain or sector, optional",
                "es": "Ámbito o sector, opcional",
                "pt": "Domínio ou setor, facultativo",
                "ar": "المجال أو القطاع، اختياري"
              },
              "type": "text",
              "options": [],
              "max_chars": 500,
              "source_extra": "Gestion de collecte — § 5.1 ; pas une question substantielle supplémentaire",
              "source": "Gestion de collecte — § 5.1 ; pas une question substantielle supplémentaire",
              "gate": {
                "field": "RA01_Participation",
                "op": "in",
                "values": [
                  1
                ]
              }
            },
            {
              "id": "RA01_Langue_texte",
              "label": {
                "fr": "Langue principale de vos réponses libres",
                "en": "Main language of your free-text responses",
                "es": "Idioma principal de sus respuestas abiertas",
                "pt": "Língua principal das suas respostas abertas",
                "ar": "اللغة الأساسية لإجاباتك الحرة"
              },
              "type": "single",
              "options": [
                {
                  "code": "fr",
                  "label": {
                    "fr": "Français",
                    "en": "French",
                    "es": "Francés",
                    "pt": "Francês",
                    "ar": "الفرنسية"
                  }
                },
                {
                  "code": "en",
                  "label": {
                    "fr": "Anglais",
                    "en": "English",
                    "es": "Inglés",
                    "pt": "Inglês",
                    "ar": "الإنجليزية"
                  }
                },
                {
                  "code": "ar",
                  "label": {
                    "fr": "Arabe",
                    "en": "Arabic",
                    "es": "Árabe",
                    "pt": "Árabe",
                    "ar": "العربية"
                  }
                },
                {
                  "code": "pt",
                  "label": {
                    "fr": "Portugais",
                    "en": "Portuguese",
                    "es": "Portugués",
                    "pt": "Português",
                    "ar": "البرتغالية"
                  }
                },
                {
                  "code": "es",
                  "label": {
                    "fr": "Espagnol",
                    "en": "Spanish",
                    "es": "Español",
                    "pt": "Espanhol",
                    "ar": "الإسبانية"
                  }
                }
              ],
              "source_extra": "Gestion de collecte — § 5.1 ; pas une question substantielle supplémentaire",
              "source": "Gestion de collecte — § 5.1 ; pas une question substantielle supplémentaire",
              "gate": {
                "field": "RA01_Participation",
                "op": "in",
                "values": [
                  1
                ]
              }
            },
            {
              "id": "RA01_Origine_invitation",
              "label": {
                "fr": "Origine de l’invitation ou du lien",
                "en": "Origin of the invitation or link",
                "es": "Origen de la invitación o del enlace",
                "pt": "Origem do convite ou da ligação",
                "ar": "مصدر الدعوة أو الرابط"
              },
              "type": "single",
              "options": [
                {
                  "code": 1,
                  "label": {
                    "fr": "INS",
                    "en": "NSO",
                    "es": "ONE",
                    "pt": "INE",
                    "ar": "الجهاز الوطني للإحصاء"
                  }
                },
                {
                  "code": 2,
                  "label": {
                    "fr": "STATAFRIC ou consultant",
                    "en": "STATAFRIC or consultant",
                    "es": "STATAFRIC o consultor",
                    "pt": "STATAFRIC ou consultor",
                    "ar": "STATAFRIC أو الاستشاري"
                  }
                },
                {
                  "code": 3,
                  "label": {
                    "fr": "Autre relais",
                    "en": "Other forwarding organisation",
                    "es": "Otra entidad que transmitió la invitación",
                    "pt": "Outra entidade que encaminhou o convite",
                    "ar": "جهة أخرى أحالت الدعوة"
                  }
                },
                {
                  "code": 4,
                  "label": {
                    "fr": "Accès spontané",
                    "en": "Unsolicited access",
                    "es": "Acceso espontáneo",
                    "pt": "Acesso espontâneo",
                    "ar": "وصول تلقائي دون دعوة مباشرة"
                  }
                },
                {
                  "code": 98,
                  "label": {
                    "fr": "Ne sait pas",
                    "en": "Do not know",
                    "es": "No sabe",
                    "pt": "Não sabe",
                    "ar": "لا أعرف"
                  }
                }
              ],
              "source_extra": "Gestion de collecte — § 5.1 ; pas une question substantielle supplémentaire",
              "source": "Gestion de collecte — § 5.1 ; pas une question substantielle supplémentaire",
              "gate": {
                "field": "RA01_Participation",
                "op": "in",
                "values": [
                  1
                ]
              }
            },
            {
              "id": "RA01_INS_relais",
              "label": {
                "fr": "INS ou organisme relais, facultatif",
                "en": "Forwarding NSO or organisation, optional",
                "es": "ONE o entidad que transmitió la invitación, opcional",
                "pt": "INE ou entidade que encaminhou o convite, facultativo",
                "ar": "الجهاز الوطني للإحصاء أو الجهة المحيلة، اختياري"
              },
              "type": "text",
              "options": [],
              "max_chars": 500,
              "source_extra": "Gestion de collecte — § 5.1 ; pas une question substantielle supplémentaire",
              "source": "Gestion de collecte — § 5.1 ; pas une question substantielle supplémentaire",
              "gate": {
                "field": "RA01_Participation",
                "op": "in",
                "values": [
                  1
                ]
              }
            },
            {
              "id": "RA01_Reference_precedente",
              "label": {
                "fr": "Référence d’une contribution précédente, si connue",
                "en": "Reference of an earlier contribution, if known",
                "es": "Referencia de una contribución anterior, si se conoce",
                "pt": "Referência de uma contribuição anterior, se conhecida",
                "ar": "مرجع مساهمة سابقة، إن كان معروفا"
              },
              "type": "text",
              "options": [],
              "max_chars": 500,
              "source": "Rapport méthodologique du 15 novembre 2026, pp. 33–33, RA01",
              "gate": {
                "field": "RA01_Participation",
                "op": "in",
                "values": [
                  1
                ]
              }
            }
          ],
          "note": {
            "fr": "",
            "en": "",
            "es": "",
            "pt": "",
            "ar": ""
          },
          "gate": null,
          "theme": "Suivi et moyens",
          "source": "Rapport méthodologique du 15 novembre 2026, pp. 33–33, RA01"
        },
        {
          "id": "RA02",
          "title": {
            "fr": "Dernière situation connue",
            "en": "Most recent known situation",
            "es": "Última situación conocida",
            "pt": "Última situação conhecida",
            "ar": "أحدث حالة معروفة"
          },
          "fields": [
            {
              "id": "RA02_Cas",
              "label": {
                "fr": "Décrivez une demande ou préparation de discours pendant la période : mois, thème, délai disponible, appui obtenu ou non. Si aucun cas n’est connu, l’indiquer. Trois lignes.",
                "en": "Describe a request or speech preparation during the period: month, topic, time available, support obtained or not. If no case is known, say so. Three lines.",
                "es": "Describa una solicitud o preparación de discurso durante el período : mes, tema, plazo disponible y apoyo obtenido o no. Si no conoce un caso, indíquelo. Tres líneas.",
                "pt": "Descreva um pedido ou preparação de discurso no período : mês, tema, prazo disponível e apoio obtido ou não. Se não conhece um caso, indique-o. Três linhas.",
                "ar": "صف طلبا أو إعداد خطاب خلال الفترة : الشهر والموضوع والمهلة المتاحة والدعم المحصل عليه أو عدمه. إذا لم تعرف حالة فاذكر ذلك. ثلاثة أسطر."
              },
              "type": "textarea",
              "options": [],
              "max_chars": 2500,
              "source": "Rapport méthodologique du 15 novembre 2026, pp. 33–33, RA02",
              "gate": {
                "field": "RA01_Participation",
                "op": "in",
                "values": [
                  1
                ]
              }
            }
          ],
          "note": {
            "fr": "",
            "en": "",
            "es": "",
            "pt": "",
            "ar": ""
          },
          "gate": null,
          "theme": "Suivi et moyens",
          "source": "Rapport méthodologique du 15 novembre 2026, pp. 33–33, RA02"
        },
        {
          "id": "RA03",
          "title": {
            "fr": "Source et contrôle",
            "en": "Source and checking",
            "es": "Fuente y control",
            "pt": "Fonte e controlo",
            "ar": "المصدر والمراجعة"
          },
          "fields": [
            {
              "id": "RA03_Source",
              "label": {
                "fr": "Pour ce cas, pouvait-on retrouver la source du chiffre ?",
                "en": "For this case, could the source of the figure be located?",
                "es": "Para este caso, ¿se podía localizar la fuente de la cifra?",
                "pt": "Para este caso, era possível localizar a fonte do número?",
                "ar": "هل كان بالإمكان العثور على مصدر الرقم لهذه الحالة؟"
              },
              "type": "single",
              "options": [
                {
                  "code": 1,
                  "label": {
                    "fr": "Oui",
                    "en": "Yes",
                    "es": "Sí",
                    "pt": "Sim",
                    "ar": "نعم"
                  }
                },
                {
                  "code": 0,
                  "label": {
                    "fr": "Non",
                    "en": "No",
                    "es": "No",
                    "pt": "Não",
                    "ar": "لا"
                  }
                },
                {
                  "code": 98,
                  "label": {
                    "fr": "Ne sait pas",
                    "en": "Do not know",
                    "es": "No sabe",
                    "pt": "Não sabe",
                    "ar": "لا أعرف"
                  }
                },
                {
                  "code": 99,
                  "label": {
                    "fr": "Non applicable",
                    "en": "Not applicable",
                    "es": "No aplicable",
                    "pt": "Não aplicável",
                    "ar": "لا ينطبق"
                  }
                }
              ],
              "source": "Rapport méthodologique du 15 novembre 2026, pp. 33–33, RA03",
              "gate": {
                "field": "RA01_Participation",
                "op": "in",
                "values": [
                  1
                ]
              }
            },
            {
              "id": "RA03_Controle",
              "label": {
                "fr": "Qui a vérifié le contenu avant transmission ? Fonction ou absence de contrôle connu.",
                "en": "Who checked the content before transmission? Function or absence of a known check.",
                "es": "¿Quién comprobó el contenido antes de enviarlo? Indique la función o la ausencia de control conocido.",
                "pt": "Quem verificou o conteúdo antes do envio? Indique a função ou a ausência de controlo conhecido.",
                "ar": "من راجع المحتوى قبل إرساله؟ اذكر الوظيفة أو عدم وجود مراجعة معروفة."
              },
              "type": "text",
              "options": [],
              "max_chars": 500,
              "source": "Rapport méthodologique du 15 novembre 2026, pp. 33–33, RA03",
              "gate": {
                "field": "RA01_Participation",
                "op": "in",
                "values": [
                  1
                ]
              }
            }
          ],
          "note": {
            "fr": "",
            "en": "",
            "es": "",
            "pt": "",
            "ar": ""
          },
          "gate": null,
          "theme": "Vérification",
          "source": "Rapport méthodologique du 15 novembre 2026, pp. 33–33, RA03"
        },
        {
          "id": "RA04",
          "title": {
            "fr": "Première amélioration",
            "en": "First improvement",
            "es": "Primera mejora",
            "pt": "Primeira melhoria",
            "ar": "أول تحسين"
          },
          "fields": [
            {
              "id": "RA04_Amelioration",
              "label": {
                "fr": "Quelle difficulté faut-il résoudre en priorité ? Quelle action serait possible avec les moyens existants et quelle fonction pourrait la conduire ? Trois lignes.",
                "en": "Which difficulty should be addressed first? What action would be possible with existing resources and which function could lead it? Three lines.",
                "es": "¿Qué dificultad debe resolverse primero? ¿Qué acción sería posible con los recursos existentes y qué función podría dirigirla? Tres líneas.",
                "pt": "Que dificuldade deve ser resolvida primeiro? Que ação seria possível com os recursos existentes e que função poderia conduzi-la? Três linhas.",
                "ar": "ما الصعوبة التي يجب حلها أولا؟ ما الإجراء الممكن بالموارد المتاحة وما الوظيفة التي يمكن أن تقوده؟ ثلاثة أسطر."
              },
              "type": "textarea",
              "options": [],
              "max_chars": 2500,
              "source": "Rapport méthodologique du 15 novembre 2026, pp. 33–33, RA04",
              "gate": {
                "field": "RA01_Participation",
                "op": "in",
                "values": [
                  1
                ]
              }
            }
          ],
          "note": {
            "fr": "",
            "en": "",
            "es": "",
            "pt": "",
            "ar": ""
          },
          "gate": null,
          "theme": "Suivi et moyens",
          "source": "Rapport méthodologique du 15 novembre 2026, pp. 33–33, RA04"
        },
        {
          "id": "RA05",
          "title": {
            "fr": "Réutilisation et suite",
            "en": "Reuse and follow-up",
            "es": "Reutilización y seguimiento",
            "pt": "Reutilização e seguimento",
            "ar": "إعادة الاستخدام والمتابعة"
          },
          "fields": [
            {
              "id": "RA05_Attribution",
              "label": {
                "fr": "Restitution autorisée",
                "en": "Authorised reporting",
                "es": "Presentación de resultados autorizada",
                "pt": "Apresentação de resultados autorizada",
                "ar": "عرض النتائج المصرح به"
              },
              "type": "single",
              "options": [
                {
                  "code": 0,
                  "label": {
                    "fr": "Synthèse anonymisée seulement, sans attribution identifiable",
                    "en": "Anonymised summary only, with no identifiable attribution",
                    "es": "Solo síntesis anonimizada, sin atribución identificable",
                    "pt": "Apenas síntese anonimizada, sem atribuição identificável",
                    "ar": "ملخص دون تحديد الهوية فقط، دون إسناد كاشف للهوية"
                  }
                },
                {
                  "code": 1,
                  "label": {
                    "fr": "Attribution à l’institution, sans citation textuelle identifiable",
                    "en": "Attribution to the institution, without an identifiable quotation",
                    "es": "Atribución a la institución, sin cita textual identificable",
                    "pt": "Atribuição à instituição, sem citação textual identificável",
                    "ar": "الإسناد إلى المؤسسة دون اقتباس نصي كاشف للهوية"
                  }
                },
                {
                  "code": 2,
                  "label": {
                    "fr": "Attribution à l’institution et citation textuelle",
                    "en": "Attribution to the institution and direct quotation",
                    "es": "Atribución a la institución y cita textual",
                    "pt": "Atribuição à instituição e citação textual",
                    "ar": "الإسناد إلى المؤسسة والاقتباس النصي"
                  }
                },
                {
                  "code": 3,
                  "label": {
                    "fr": "Me soumettre le passage avant toute attribution ou citation identifiable",
                    "en": "Submit the passage to me before any identifiable attribution or quotation",
                    "es": "Enviarme el pasaje antes de cualquier atribución o cita identificable",
                    "pt": "Enviar-me o trecho antes de qualquer atribuição ou citação identificável",
                    "ar": "اعرض علي المقطع قبل أي إسناد أو اقتباس كاشف للهوية"
                  }
                }
              ],
              "attribution": true,
              "source": "Rapport méthodologique du 15 novembre 2026, pp. 33–33, RA05",
              "gate": {
                "field": "RA01_Participation",
                "op": "in",
                "values": [
                  1
                ]
              }
            },
            {
              "id": "RA05_Lien",
              "label": {
                "fr": "Lien public utile, facultatif",
                "en": "Useful public link, optional",
                "es": "Enlace público útil, opcional",
                "pt": "Ligação pública útil, facultativa",
                "ar": "رابط عام مفيد، اختياري"
              },
              "type": "text",
              "options": [],
              "max_chars": 2000,
              "source": "Rapport méthodologique du 15 novembre 2026, pp. 33–33, RA05",
              "gate": {
                "field": "RA01_Participation",
                "op": "in",
                "values": [
                  1
                ]
              }
            },
            {
              "id": "RA05_Contact_fonction",
              "label": {
                "fr": "Contact institutionnel pour un complément : fonction ou service, facultatif",
                "en": "Institutional follow-up contact: function or service, optional",
                "es": "Contacto institucional para aclaraciones : función o servicio, opcional",
                "pt": "Contacto institucional para esclarecimentos : função ou serviço, facultativo",
                "ar": "جهة اتصال مؤسسية للاستكمال : الوظيفة أو الإدارة، اختياري"
              },
              "type": "text",
              "options": [],
              "max_chars": 500,
              "private": true,
              "source": "Rapport méthodologique du 15 novembre 2026, pp. 33–33, RA05",
              "gate": {
                "field": "RA01_Participation",
                "op": "in",
                "values": [
                  1
                ]
              }
            },
            {
              "id": "RA05_Contact_courriel",
              "label": {
                "fr": "Contact institutionnel pour un complément, facultatif (courriel)",
                "en": "Institutional contact for follow-up, optional (email)",
                "es": "Contacto institucional para aclaraciones, opcional (correo electrónico)",
                "pt": "Contacto institucional para esclarecimentos, facultativo (correio eletrónico)",
                "ar": "جهة اتصال مؤسسية للاستكمال، اختياري (البريد الإلكتروني)"
              },
              "type": "email",
              "options": [],
              "max_chars": 254,
              "private": true,
              "source": "Rapport méthodologique du 15 novembre 2026, pp. 33–33, RA05",
              "gate": {
                "field": "RA01_Participation",
                "op": "in",
                "values": [
                  1
                ]
              }
            }
          ],
          "note": {
            "fr": "Les choix 1 et 2 supposent l’autorisation correspondante. Une contribution individuelle sans cette autorisation reste anonymisée. Sans choix explicite, la synthèse anonymisée (code 0) s’applique. L’absence de retour ne vaut jamais accord de citation.",
            "en": "Options 1 and 2 require the corresponding authorisation. An individual contribution without that authorisation remains anonymised. Without an explicit choice, an anonymised summary (code 0) applies. Silence never constitutes permission to quote.",
            "es": "Las opciones 1 y 2 requieren la autorización correspondiente. Una contribución individual sin esa autorización permanece anonimizada. Sin selección explícita, se aplica la síntesis anonimizada (código 0). El silencio nunca autoriza una cita.",
            "pt": "As opções 1 e 2 exigem a autorização correspondente. Uma contribuição individual sem essa autorização mantém-se anonimizada. Sem escolha explícita, aplica-se a síntese anonimizada (código 0). O silêncio nunca autoriza uma citação.",
            "ar": "يتطلب الخياران 1 و2 التصريح المناسب. تبقى المساهمة الفردية دون هذا التصريح مجهولة الهوية. عند غياب اختيار صريح، يطبق الملخص دون تحديد الهوية (الرمز 0). الصمت لا يعني أبدا الإذن بالاقتباس."
          },
          "gate": null,
          "theme": "Suivi et moyens",
          "source": "Rapport méthodologique du 15 novembre 2026, pp. 33–33, RA05"
        }
      ],
      "consent": "RA01_Participation",
      "attribution": "RA05_Attribution",
      "institution": "RA01_Institution",
      "country": "RA01_Pays",
      "status": "RA01_Statut",
      "version": "1.2 — 2026-11-15",
      "unit": "RA01_Unite",
      "sector": "RA01_Domaine",
      "response_language": "RA01_Langue_texte",
      "origin": "RA01_Origine_invitation",
      "relay": "RA01_INS_relais",
      "previous": "RA01_Reference_precedente",
      "profile": "Abrégé",
      "priority_prefix": null
    }
  },
  "notice": {
    "fr": "STATAFRIC et le consultant Mamadou Niang préparent des outils pratiques pour l’appui statistique aux discours et à la décision. Référence des pratiques : du 1er novembre 2025 au 31 octobre 2026 ; dater séparément les évolutions ultérieures. Collecte : du 19 au 26 novembre 2026 inclus. Les réponses réelles alimenteront L3, jamais L2 qui reste documentaire.\n\nParticipation volontaire. Une réponse institutionnelle, de service ou individuelle est recevable si son périmètre est précisé. Répondre directement à l’équipe de la mission, sans approbation préalable de l’INS relais. Les restitutions sont agrégées ou anonymisées par défaut ; attribution ou citation identifiable selon votre choix à la dernière rubrique. Les pièces remises au Client respectent ces autorisations. Un cas particulier peut rester reconnaissable. Les coordonnées facultatives sont séparées de l’analyse.\n\nNe transmettre ni opinions politiques, données individuelles, coordonnées personnelles inutiles ni document confidentiel. Utiliser des liens publics ou extraits non confidentiels autorisés. Une contribution individuelle n’engage pas l’institution.\n\nUne réponse partielle est utile. Aucun choix substantiel n’est précoché. Un blanc ne signifie ni « non » ni zéro. « Aucun », « ne sait pas » et « non applicable » sont exclusifs des autres réponses lorsque le questionnaire le prévoit.\n\nAvant l’envoi, la saisie reste dans la session, pas dans la base. Elle peut être perdue à la fermeture : télécharger un brouillon pour reprendre. Un brouillon ne vaut pas envoi. Seul un accusé confirmé atteste l’enregistrement. Les réponses originales restent protégées ; seuls le consultant et les personnes habilitées y accèdent. La langue peut être changée sans créer une nouvelle contribution. En cas de difficulté, utiliser le formulaire de repli dans la même langue et l’envoyer au contact de la mission.",
    "en": "STATAFRIC and consultant Mamadou Niang are preparing practical tools for statistical support to speeches and decision-making. Reference period for practices: 1 November 2025 to 31 October 2026; date subsequent changes separately. Collection: 19 to 26 November 2026 inclusive. Actual responses will inform L3, never the exclusively desk-based L2.\n\nParticipation is voluntary. Institutional, service-level and individual contributions are accepted when their scope is clear. Respond directly to the assignment team, without prior approval by the forwarding NSO. Reporting is aggregated or anonymised by default; identifiable attribution or quotation follows your choice in the last section. Materials handed to the Client respect these permissions. A distinctive case may remain recognisable. Optional contact details are separate from analysis.\n\nDo not provide political opinions, individual-level data, unnecessary personal details or confidential documents. Use public links or authorised non-confidential extracts. An individual contribution does not commit the institution.\n\nA partial response is useful. No substantive option is preselected. A blank is neither no nor zero. None, do not know and not applicable exclude other answers where the questionnaire specifies.\n\nBefore submission, entries remain in the session, not in the database. They may be lost on closing: download a draft to resume. A draft is not a submission. Only a confirmed receipt attests to recording. Original responses are protected; only the consultant and authorised persons can access them. Changing language does not create a new contribution. If needed, use the fallback form in the same language and email it to the assignment contact.",
    "es": "STATAFRIC y el consultor Mamadou Niang preparan herramientas prácticas de apoyo estadístico a los discursos y la toma de decisiones. Período de referencia : del 1 de noviembre de 2025 al 31 de octubre de 2026; feche los cambios posteriores por separado. Recogida : del 19 al 26 de noviembre de 2026 inclusive. Las respuestas reales alimentarán L3, nunca L2, que es exclusivamente documental.\n\nLa participación es voluntaria. Se aceptan contribuciones institucionales, de servicios e individuales cuyo ámbito esté claro. Responda directamente al equipo de la misión, sin aprobación previa de la ONE que remitió la invitación. Los resultados se presentan agregados o anonimizados por defecto; la atribución o cita identificable sigue su elección en la última sección. Los documentos entregados al Cliente respetarán esas autorizaciones. Un caso singular puede seguir siendo reconocible. Los contactos opcionales se separan del análisis.\n\nNo proporcione opiniones políticas, datos individuales, información personal innecesaria ni documentos confidenciales. Use enlaces públicos o extractos no confidenciales autorizados. Una contribución individual no compromete a la institución.\n\nUna respuesta parcial es útil. Ninguna opción sustantiva está preseleccionada. Un blanco no es un no ni un cero. « Ninguno », « No sabe » y « No aplicable » excluyen otras respuestas cuando así lo indica el cuestionario.\n\nAntes del envío, los datos permanecen en la sesión, no en la base. Pueden perderse al cerrar : descargue un borrador para continuar. Un borrador no constituye un envío. Solo un acuse confirmado acredita el registro. Las respuestas originales están protegidas y solo son accesibles al consultor y a las personas autorizadas. Cambiar de idioma no crea otra contribución. Si es necesario, use el formulario alternativo en el mismo idioma y envíelo por correo al contacto de la misión.",
    "pt": "O STATAFRIC e o consultor Mamadou Niang preparam ferramentas práticas de apoio estatístico aos discursos e à decisão. Período de referência : de 1 de novembro de 2025 a 31 de outubro de 2026; date separadamente as alterações posteriores. Recolha : de 19 a 26 de novembro de 2026, inclusive. As respostas reais alimentarão L3, nunca L2, que é exclusivamente documental.\n\nA participação é voluntária. Aceitam-se contribuições institucionais, de serviços e individuais com âmbito claro. Responda diretamente à equipa da missão, sem aprovação prévia do INE que encaminhou o convite. Os resultados são agregados ou anonimizados por defeito; a atribuição ou citação identificável segue a escolha na última secção. As peças entregues ao Cliente respeitam essas autorizações. Um caso singular pode continuar reconhecível. Os contactos facultativos são separados da análise.\n\nNão forneça opiniões políticas, dados individuais, dados pessoais desnecessários nem documentos confidenciais. Use ligações públicas ou excertos não confidenciais autorizados. Uma contribuição individual não compromete a instituição.\n\nUma resposta parcial é útil. Nenhuma opção substantiva está pré-selecionada. Um campo vazio não significa não nem zero. « Nenhum », « Não sabe » e « Não aplicável » excluem outras respostas quando o questionário o indica.\n\nAntes do envio, os dados permanecem na sessão, não na base. Podem perder-se ao fechar : descarregue um rascunho para retomar. Um rascunho não constitui envio. Apenas um recibo confirmado comprova o registo. As respostas originais são protegidas; só o consultor e pessoas autorizadas lhes acedem. Mudar de língua não cria nova contribuição. Se necessário, use o formulário alternativo na mesma língua e envie-o por correio ao contacto da missão.",
    "ar": "يعد STATAFRIC والاستشاري مامادو نيانغ أدوات عملية للدعم الإحصائي للخطابات واتخاذ القرار. الفترة المرجعية للممارسات : من 1 نوفمبر 2025 إلى 31 أكتوبر 2026؛ تؤرخ التغييرات اللاحقة بشكل منفصل. الجمع : من 19 إلى 26 نوفمبر 2026 شاملين. تستخدم الإجابات الفعلية في L3، وليس في L2 الذي يظل وثائقيا حصرا.\n\nالمشاركة طوعية. تقبل المساهمات المؤسسية ومساهمات الإدارات والأفراد متى كان نطاقها واضحا. أجب مباشرة لفريق المهمة، دون موافقة مسبقة من الجهاز الوطني للإحصاء المحيل. تعرض النتائج مجمعة أو دون تحديد الهوية افتراضيا؛ ويخضع الإسناد أو الاقتباس الكاشف للهوية لاختيارك في القسم الأخير. تحترم الوثائق المسلمة للعميل هذه الأذونات. قد تظل حالة مميزة قابلة للتعرف عليها. تحفظ بيانات الاتصال الاختيارية منفصلة عن التحليل.\n\nلا تقدم آراء سياسية أو بيانات فردية أو بيانات شخصية غير ضرورية أو وثائق سرية. استخدم روابط عامة أو مقتطفات غير سرية مصرحا بها. المساهمة الفردية لا تلزم المؤسسة.\n\nالإجابة الجزئية مفيدة. لا يحدد أي خيار موضوعي مسبقا. الفراغ لا يعني لا أو صفرا. تستبعد خيارات « لا يوجد » و« لا أعرف » و« لا ينطبق » بقية الإجابات عندما ينص الاستبيان على ذلك.\n\nقبل الإرسال، تبقى البيانات في الجلسة لا في القاعدة، وقد تفقد عند الإغلاق : نزل مسودة للاستئناف. المسودة ليست إرسالا. وحده إشعار استلام مؤكد يثبت التسجيل. الإجابات الأصلية محمية ولا يطلع عليها إلا الاستشاري والمخولون. تغيير اللغة لا ينشئ مساهمة جديدة. عند الحاجة، استخدم الاستمارة البديلة باللغة نفسها وأرسلها بالبريد الإلكتروني إلى جهة اتصال المهمة."
  }
}''')


# --- ui_catalog.py ---
# Documentation interne non affichee.
UI={}
def add(key,fr,en,es,pt,ar): UI[key]=dict(zip(('fr','en','es','pt','ar'),(fr,en,es,pt,ar)))
add('title','Appui statistique aux discours des décideurs','Statistical support for decision-makers’ speeches','Apoyo estadístico a los discursos de los decisores','Apoio estatístico aos discursos dos decisores','الدعم الإحصائي لخطابات صانعي القرار')
add('language','Langue','Language','Idioma','Língua','اللغة')
add('form','Votre questionnaire','Your questionnaire','Su cuestionario','O seu questionário','استبيانك')
add('preview','Prévisualisation : aucun envoi possible. Ne saisir aucune donnée réelle.','Preview: submission is disabled. Do not enter real data.','Vista previa : envío desactivado. No introduzca datos reales.','Pré-visualização : envio desativado. Não introduza dados reais.','معاينة : الإرسال معطل. لا تدخل بيانات حقيقية.')
add('test','MODE TEST — réponses fictives uniquement, exclues du diagnostic réel.','TEST MODE — fictitious responses only, excluded from the actual diagnostic.','MODO DE PRUEBA — solo respuestas ficticias, excluidas del diagnóstico real.','MODO DE TESTE — apenas respostas fictícias, excluídas do diagnóstico real.','وضع الاختبار — إجابات وهمية فقط، مستبعدة من التشخيص الفعلي.')
add('dates','Référence : 01/11/2025–31/10/2026. Collecte : 19–26 novembre 2026 inclus.','Reference: 1 Nov 2025–31 Oct 2026. Collection: 19–26 November 2026 inclusive.','Referencia : 01/11/2025–31/10/2026. Recogida : 19–26 de noviembre de 2026 inclusive.','Referência : 01/11/2025–31/10/2026. Recolha : 19–26 de novembro de 2026 inclusive.','الفترة المرجعية : 01/11/2025–31/10/2026. الجمع : من 19 إلى 26 نوفمبر 2026 شاملين.')
add('contact','Contact de la mission','Assignment contact','Contacto de la misión','Contacto da missão','جهة اتصال المهمة')
add('notice','Notice de participation et protection des réponses','Participation notice and response protection','Aviso de participación y protección de respuestas','Aviso de participação e proteção das respostas','إشعار المشاركة وحماية الإجابات')
add('empty','— Non renseigné —','— Not answered —','— Sin responder —','— Não respondido —','— دون إجابة —')
add('max_choices','Au plus {n} choix.','Up to {n} choices.','Hasta {n} opciones.','Até {n} escolhas.','حتى {n} اختيارات.')
add('words','Repère : {n} mots au plus. Le texte original est conservé.','Guideline: up to {n} words. Original text is retained.','Orientación : hasta {n} palabras. Se conserva el original.','Referência : até {n} palavras. O original é conservado.','إرشاد : حتى {n} كلمة. يحفظ النص الأصلي.')
add('previous','Précédent','Previous','Anterior','Anterior','السابق')
add('next','Suivant','Next','Siguiente','Seguinte','التالي')
add('review','Relire et envoyer','Review and submit','Revisar y enviar','Rever e enviar','المراجعة والإرسال')
add('review_title','Relecture avant envoi','Review before submission','Revisión antes del envío','Revisão antes do envio','المراجعة قبل الإرسال')
add('edit','Modifier cette rubrique','Edit this section','Modificar esta sección','Alterar esta secção','تعديل هذا القسم')
add('progress','Rubrique {n} sur {total}','Section {n} of {total}','Sección {n} de {total}','Secção {n} de {total}','القسم {n} من {total}')
add('partial','Une réponse partielle est recevable. Vérifiez les codes, la période, les priorités et les droits de citation.','Partial responses are accepted. Check codes, reference period, priorities and quotation permissions.','Se aceptan respuestas parciales. Revise códigos, período, prioridades y permisos de cita.','Aceitam-se respostas parciais. Verifique códigos, período, prioridades e permissões de citação.','تقبل الإجابات الجزئية. راجع الرموز والفترة والأولويات وأذونات الاقتباس.')
add('short','Cette réponse abrégée reste partielle. Aucune réponse omise ne sera déduite.','This abbreviated response remains partial. Omitted answers will not be inferred.','Esta respuesta abreviada es parcial. No se inferirán respuestas omitidas.','Esta resposta abreviada é parcial. Não se inferem respostas omitidas.','تبقى هذه الإجابة المختصرة جزئية. لن تستنتج الإجابات المحذوفة.')
add('indeterminate','Cette rubrique dépend d’une réponse amont non renseignée. Revenez en arrière ou poursuivez avec une contribution partielle.','This section depends on an unanswered earlier item. Go back or continue with a partial contribution.','Esta sección depende de una respuesta anterior no indicada. Vuelva atrás o continúe con una contribución parcial.','Esta secção depende de uma resposta anterior em falta. Volte atrás ou continue com uma contribuição parcial.','يعتمد هذا القسم على إجابة سابقة غير محددة. ارجع أو تابع بمساهمة جزئية.')
add('refused','Votre choix est respecté. Aucune réponse n’est enregistrée.','Your choice is respected. No response is recorded.','Se respeta su decisión. No se registra ninguna respuesta.','A sua decisão é respeitada. Nenhuma resposta é registada.','يحترم اختيارك. لا تسجل أي إجابة.')
add('ineligible','Ce parcours concerne les producteurs. Choisissez « Équipes préparant les discours » pour un rôle d’utilisateur.','This pathway is for producers. Choose “Speech-preparation teams” for a user role.','Este recorrido es para productores. Elija « Equipos que preparan discursos » si es usuario.','Este percurso é para produtores. Escolha « Equipas que preparam discursos » se é utilizador.','هذا المسار للمنتجين. اختر « فرق إعداد الخطابات » لدور المستخدم.')
add('authority','Je confirme disposer de l’autorisation institutionnelle correspondant à mon choix d’attribution ou de citation.','I confirm that I hold the institutional authorisation required for my attribution or quotation choice.','Confirmo que dispongo de la autorización institucional requerida para mi elección de atribución o cita.','Confirmo que disponho da autorização institucional exigida para a escolha de atribuição ou citação.','أؤكد حيازتي التصريح المؤسسي المطلوب لخيار الإسناد أو الاقتباس الذي حددته.')
add('anonymous','Sans choix explicite, seule une synthèse anonymisée est autorisée. Le choix 3 exige un accord ultérieur sur le passage.','Without an explicit choice, only an anonymised summary is allowed. Option 3 requires later approval of the passage.','Sin elección explícita, solo se autoriza una síntesis anonimizada. La opción 3 exige aprobación posterior del pasaje.','Sem escolha explícita, só é autorizada uma síntese anonimizada. A opção 3 exige aprovação posterior do trecho.','دون اختيار صريح، يسمح فقط بملخص دون تحديد الهوية. يتطلب الخيار 3 موافقة لاحقة على المقطع.')
add('confirm','Je souhaite transmettre cette contribution à l’équipe de la mission.','I wish to submit this contribution to the assignment team.','Deseo enviar esta contribución al equipo de la misión.','Desejo enviar esta contribuição à equipa da missão.','أرغب في إرسال هذه المساهمة إلى فريق المهمة.')
add('submit','Envoyer ma contribution','Submit my contribution','Enviar mi contribución','Enviar a minha contribuição','إرسال مساهمتي')
add('closed','La campagne n’est pas ouverte à cet instant. Conservez votre brouillon.','The campaign is not currently open. Keep your draft.','La campaña no está abierta en este momento. Guarde su borrador.','A campanha não está aberta neste momento. Guarde o rascunho.','الحملة غير مفتوحة حاليا. احتفظ بمسودتك.')
add('access','Code d’accès reçu dans l’invitation, si demandé','Access code from the invitation, if required','Código de acceso de la invitación, si se requiere','Código de acesso do convite, se solicitado','رمز الوصول الوارد في الدعوة، عند طلبه')
add('success','Votre contribution a été enregistrée. Merci.','Your contribution has been recorded. Thank you.','Su contribución ha sido registrada. Gracias.','A sua contribuição foi registada. Obrigado.','سجلت مساهمتك. شكرا.')
add('success_test','Votre réponse de test a été enregistrée.','Your test response has been recorded.','Su respuesta de prueba ha sido registrada.','A sua resposta de teste foi registada.','سجلت إجابتك الاختبارية.')
add('receipt','Télécharger mon accusé et mes réponses','Download my receipt and responses','Descargar mi acuse y respuestas','Descarregar o recibo e as respostas','تنزيل إشعار الاستلام وإجاباتي')
add('receipt_title','Accusé de réception','Receipt','Acuse de recibo','Recibo','إشعار الاستلام')
add('new','Commencer une nouvelle contribution','Start a new contribution','Iniciar otra contribución','Iniciar nova contribuição','بدء مساهمة جديدة')
add('supplement','Pour un complément, créez une nouvelle contribution et rappelez votre référence. Aucun envoi reçu n’est écrasé.','For a supplement, create a new contribution and refer to your receipt. No received response is overwritten.','Para complementar, cree otra contribución y cite su referencia. No se sobrescribe una respuesta recibida.','Para complementar, crie nova contribuição e indique a referência. Nenhuma resposta recebida é substituída.','للاستكمال، أنشئ مساهمة جديدة واذكر مرجعك. لا تستبدل أي إجابة مستلمة.')
add('not_validation','La réception ne vaut ni validation du contenu ni approbation institutionnelle.','Receipt is neither content validation nor institutional approval.','La recepción no valida el contenido ni supone aprobación institucional.','A receção não valida o conteúdo nem constitui aprovação institucional.','الاستلام لا يعني اعتماد المحتوى أو الموافقة المؤسسية.')
add('resume','Reprendre un brouillon ou un envoi en attente','Resume a draft or pending submission','Retomar borrador o envío pendiente','Retomar rascunho ou envio pendente','استئناف مسودة أو إرسال معلق')
add('upload','Fichier JSON produit par cette version','JSON file produced by this version','Archivo JSON generado por esta versión','Ficheiro JSON produzido por esta versão','ملف JSON صادر عن هذه النسخة')
add('import','Importer le fichier','Import file','Importar archivo','Importar ficheiro','استيراد الملف')
add('draft','Conserver mon brouillon','Keep my draft','Guardar mi borrador','Guardar o meu rascunho','حفظ مسودتي')
add('download_draft','Télécharger le brouillon JSON','Download JSON draft','Descargar borrador JSON','Descarregar rascunho JSON','تنزيل مسودة JSON')
add('draft_warning','Ce fichier peut contenir vos coordonnées. Gardez-le sur un appareil sûr. Il ne vaut pas envoi.','This file may contain contact details. Keep it on a safe device. It is not a submission.','El archivo puede contener contactos. Guárdelo de forma segura. No constituye envío.','O ficheiro pode conter contactos. Guarde-o em segurança. Não constitui envio.','قد يحتوي الملف على بيانات الاتصال. احفظه على جهاز آمن. لا يعد إرسالا.')
add('pending','La réception n’est pas confirmée. Réessayez exactement le même envoi : son identifiant empêche les doublons.','Receipt is not confirmed. Retry the exact same submission: its identifier prevents duplicates.','La recepción no está confirmada. Reintente exactamente el mismo envío : su identificador evita duplicados.','A receção não está confirmada. Repita exatamente o mesmo envio : o identificador evita duplicados.','لم يؤكد الاستلام. أعد الإرسال نفسه تماما : يمنع معرفه التكرار.')
add('pending_download','Conserver l’envoi en attente','Keep pending submission','Guardar envío pendiente','Guardar envio pendente','حفظ الإرسال المعلق')
add('retry','Réessayer le même envoi','Retry the same submission','Reintentar el mismo envío','Repetir o mesmo envio','إعادة الإرسال نفسه')
add('invalid_access','Code d’accès incorrect.','Incorrect access code.','Código de acceso incorrecto.','Código de acesso incorreto.','رمز الوصول غير صحيح.')
add('conflict','Cet identifiant existe avec un contenu différent. Contactez l’équipe sans écraser la réponse.','This identifier exists with different content. Contact the team without overwriting the response.','Este identificador existe con otro contenido. Contacte al equipo sin sobrescribir la respuesta.','Este identificador existe com outro conteúdo. Contacte a equipa sem substituir a resposta.','يوجد هذا المعرف بمحتوى مختلف. تواصل مع الفريق دون استبدال الإجابة.')
add('service_error','Le service n’a pas confirmé l’opération. Conservez le brouillon et contactez l’équipe.','The service has not confirmed the operation. Keep the draft and contact the team.','El servicio no confirmó la operación. Guarde el borrador y contacte al equipo.','O serviço não confirmou a operação. Guarde o rascunho e contacte a equipa.','لم يؤكد النظام العملية. احتفظ بالمسودة وتواصل مع الفريق.')
add('unreviewed','Cette langue doit encore être autorisée pour la collecte réelle. Le formulaire est consultable en prévisualisation.','This language still requires approval for live collection. The form can be previewed.','Este idioma aún requiere autorización para la recogida real. Puede consultar el formulario.','Esta língua ainda requer autorização para a recolha real. Pode consultar o formulário.','لا تزال هذه اللغة تحتاج إلى اعتماد للجمع الفعلي. يمكن معاينة الاستمارة.')
add('fallback','Formulaire de repli à remplir et retourner par e-mail','Fallback form to complete and return by email','Formulario alternativo para completar y enviar por correo','Formulário alternativo para preencher e enviar por correio','استمارة بديلة لتعبئتها وإعادتها بالبريد الإلكتروني')
add('no_consent','L’accord de participation est nécessaire pour envoyer.','Participation consent is required to submit.','Se requiere consentimiento para enviar.','É necessário consentimento para enviar.','تلزم الموافقة على المشاركة للإرسال.')
add('bad_code','Code non autorisé.','Invalid code.','Código no válido.','Código inválido.','رمز غير مسموح به.')
add('bad_selection','Sélection non autorisée ou dupliquée.','Invalid or duplicate selection.','Selección no válida o duplicada.','Seleção inválida ou duplicada.','اختيار غير مسموح به أو مكرر.')
add('too_many','Nombre maximal de choix dépassé.','Maximum number of choices exceeded.','Superado el máximo de opciones.','Máximo de escolhas excedido.','تجاوز الحد الأقصى للاختيارات.')
add('exclusive','« Aucun », « Ne sait pas » et « Non applicable » doivent rester seuls.','None, Do not know and Not applicable must be selected alone.','Ninguno, No sabe y No aplicable deben seleccionarse solos.','Nenhum, Não sabe e Não aplicável devem ser selecionados isoladamente.','يجب اختيار « لا يوجد » و« لا أعرف » و« لا ينطبق » منفردة.')
add('text_required','Texte attendu ou longueur maximale dépassée.','Text expected or maximum length exceeded.','Se esperaba texto o se superó la longitud máxima.','Esperava-se texto ou foi excedido o comprimento máximo.','يلزم نص أو تم تجاوز الطول الأقصى.')
add('email_error','Courriel invalide : corriger ou laisser vide.','Invalid email: correct or leave blank.','Correo no válido : corríjalo o déjelo vacío.','Correio inválido : corrija ou deixe vazio.','بريد غير صالح : صححه أو اتركه فارغا.')
add('month_error','Utiliser MM/AAAA, par exemple 10/2026.','Use MM/YYYY, for example 10/2026.','Use MM/AAAA, por ejemplo 10/2026.','Use MM/AAAA, por exemplo 10/2026.','استخدم MM/YYYY، مثلا 10/2026.')
add('month_warning','Date hors période de référence : vérifier ou préciser.','Date outside the reference period: check or clarify.','Fecha fuera del período : compruebe o aclare.','Data fora do período : verifique ou esclareça.','التاريخ خارج الفترة المرجعية : راجع أو وضح.')
add('word_warning','Longueur indicative dépassée : raccourcir si possible; le texte n’est pas tronqué.','Suggested length exceeded: shorten if possible; text is not truncated.','Extensión orientativa superada : reduzca si es posible; no se trunca el texto.','Extensão indicativa excedida : reduza se possível; o texto não é truncado.','تم تجاوز الطول الإرشادي : اختصر إن أمكن؛ لا يقتطع النص.')
add('priority_error','Classer des priorités distinctes, dans l’ordre et sans rang vide.','Rank distinct priorities in order, without an empty rank.','Ordene prioridades distintas sin dejar posiciones vacías.','Ordene prioridades distintas sem posições vazias.','رتب أولويات مختلفة بالتتابع دون رتبة فارغة.')
add('volume_error','Aucune demande et présence d’une demande urgente sont incompatibles.','No requests and an urgent request are inconsistent.','Ninguna solicitud y una solicitud urgente son incompatibles.','Nenhum pedido e um pedido urgente são incompatíveis.','غياب الطلبات ووجود طلب عاجل غير متوافقين.')
add('deadline_warning','Aucune échéance déclarée mais respect du délai évalué : vérifier le code 99.','No deadline declared but compliance assessed: check code 99.','Sin plazo declarado pero cumplimiento valorado : revise el código 99.','Sem prazo declarado mas cumprimento avaliado : verifique o código 99.','لم تحدد مهلة لكن قيم احترامها : راجع الرمز 99.')
add('support_error','Harmoniser l’existence ou l’absence d’un appui demandé à l’INS.','Make the presence or absence of an NSO support request consistent.','Armonice la existencia o ausencia de una solicitud a la ONE.','Harmonize a existência ou ausência de um pedido ao INE.','وحد الإجابة بشأن وجود أو غياب طلب دعم من الجهاز الوطني للإحصاء.')
add('other_warning','La précision « Autre » est utile mais facultative.','Specifying Other is useful but optional.','Especificar Otra es útil pero opcional.','Especificar Outra é útil mas facultativo.','تحديد « أخرى » مفيد لكنه اختياري.')
add('content_required','Renseigner au moins une information en plus du consentement.','Provide at least one item besides consent.','Indique al menos un dato además del consentimiento.','Indique pelo menos um dado além do consentimento.','قدم معلومة واحدة على الأقل إضافة إلى الموافقة.')
add('invalid_file','Fichier invalide ou version incompatible. Aucun fichier n’a été exécuté.','Invalid file or incompatible version. No file was executed.','Archivo no válido o versión incompatible. No se ejecutó ningún archivo.','Ficheiro inválido ou versão incompatível. Nenhum ficheiro foi executado.','ملف غير صالح أو نسخة غير متوافقة. لم ينفذ أي ملف.')
add('reference','Référence','Reference','Referencia','Referência','المرجع')
add('version','Version','Version','Versión','Versão','النسخة')
add('relay_title','Relais des INS — invitations et liens','NSO forwarding — invitations and links','Transmisión por las ONE — invitaciones y enlaces','Encaminhamento pelos INE — convites e ligações','إحالة الأجهزة الوطنية للإحصاء — الدعوات والروابط')
add('relay_warning','Cet outil prépare les invitations. Il n’envoie aucun courriel et ne donne aucun accès aux réponses.','This tool prepares invitations. It sends no emails and gives no access to responses.','Esta herramienta prepara invitaciones. No envía correos ni da acceso a respuestas.','Esta ferramenta prepara convites. Não envia correios nem dá acesso a respostas.','تعد هذه الأداة الدعوات. لا ترسل بريدا ولا تتيح الوصول إلى الإجابات.')
add('base_url','URL réelle de l’application de collecte','Actual collection application URL','URL real de la aplicación de recogida','URL real da aplicação de recolha','الرابط الفعلي لتطبيق الجمع')
add('relay_id','Identifiant du relais, facultatif et non secret','Forwarding identifier, optional and not secret','Identificador de transmisión, opcional y no secreto','Identificador de encaminhamento, facultativo e não secreto','معرف الإحالة، اختياري وغير سري')
add('institution','Institution','Institution','Institución','Instituição','المؤسسة')
add('sector','Domaine ou secteur','Domain or sector','Ámbito o sector','Domínio ou setor','المجال أو القطاع')
add('date_sent','Date réelle d’envoi (laisser vide si préparé seulement)','Actual sending date (blank if only prepared)','Fecha real de envío (vacía si solo está preparada)','Data real de envio (vazia se apenas preparado)','تاريخ الإرسال الفعلي (فارغ إذا أعدت الدعوة فقط)')
add('invite','Préparer une invitation','Prepare an invitation','Preparar invitación','Preparar convite','إعداد دعوة')
add('list_download','Télécharger le relevé des invitations','Download invitation register','Descargar registro de invitaciones','Descarregar registo de convites','تنزيل سجل الدعوات')
add('add_invite','Ajouter au relevé','Add to register','Añadir al registro','Adicionar ao registo','إضافة إلى السجل')
add('link','Lien à transmettre','Link to forward','Enlace para transmitir','Ligação a encaminhar','الرابط المطلوب إحالته')
add('invitation_text','Madame, Monsieur,\nNotre INS vous invite à contribuer au diagnostic STATAFRIC sur l’appui statistique aux discours et à la décision. Merci de répondre directement via le lien ci-dessous du 19 au 26 novembre 2026 inclus. La participation est volontaire, dans la langue de votre choix. Votre réponse n’est pas soumise à notre validation. Ne partager aucun contenu confidentiel. La notice précise les autorisations.\n{link}\nMerci.','Dear colleague,\nOur NSO invites you to contribute to the STATAFRIC diagnostic on statistical support to speeches and decision-making. Please respond directly through the link below from 19 to 26 November 2026 inclusive. Participation is voluntary, in your chosen language. Your response is not subject to our approval. Do not share confidential content. Permissions are explained in the notice.\n{link}\nThank you.','Estimado/a colega,\nNuestra ONE le invita al diagnóstico STATAFRIC sobre apoyo estadístico a discursos y decisiones. Responda directamente mediante el enlace del 19 al 26 de noviembre de 2026 inclusive. La participación es voluntaria, en el idioma elegido. Su respuesta no requiere nuestra aprobación. No comparta contenido confidencial. El aviso explica las autorizaciones.\n{link}\nGracias.','Caro/a colega,\nO nosso INE convida-o/a ao diagnóstico STATAFRIC sobre apoio estatístico aos discursos e à decisão. Responda diretamente pela ligação entre 19 e 26 de novembro de 2026, inclusive. A participação é voluntária, na língua escolhida. A resposta não depende da nossa aprovação. Não partilhe conteúdo confidencial. O aviso explica as autorizações.\n{link}\nObrigado.','الزميلة أو الزميل المحترم،\nيدعوكم جهازنا الوطني للإحصاء للمساهمة في تشخيص STATAFRIC للدعم الإحصائي للخطابات واتخاذ القرار. يرجى الإجابة مباشرة عبر الرابط من 19 إلى 26 نوفمبر 2026 شاملين. المشاركة طوعية وباللغة المختارة، ولا تخضع إجابتكم لموافقتنا. لا تشاركوا محتوى سريا. يوضح الإشعار الأذونات.\n{link}\nشكرا.')
add('planned','Préparé','Prepared','Preparada','Preparado','معدة')
add('sent','Envoyé','Sent','Enviada','Enviado','مرسلة')
add('error_invite','Renseigner une institution et une URL HTTPS valide. Vérifier le format de date AAAA-MM-JJ.','Enter an institution and valid HTTPS URL. Check date format YYYY-MM-DD.','Indique institución y URL HTTPS válida. Revise la fecha AAAA-MM-DD.','Indique instituição e URL HTTPS válido. Verifique a data AAAA-MM-DD.','أدخل مؤسسة ورابط HTTPS صالحا. راجع صيغة التاريخ YYYY-MM-DD.')
add('local_warning','Mode local de test : ne pas utiliser de données réelles.','Local test mode: do not use real data.','Modo local de prueba : no use datos reales.','Modo local de teste : não use dados reais.','وضع اختبار محلي : لا تستخدم بيانات حقيقية.')

add('logic_and','ET','AND','Y','E','و')
add('logic_or','OU','OR','O','OU','أو')
add('condition','Condition d’affichage','Display condition','Condición de visualización','Condição de apresentação','شرط العرض')
add('fallback_filter_guide','Formulaire de repli : conserver les codes. Les conditions affichées traduisent les filtres : ∈ signifie « parmi », ∉ « différent de », ∩ ≠ ∅ « contient au moins un ». En cas de condition non remplie, ne pas répondre au champ. Une réponse partielle est admise. Après refus, arrêter sans autre réponse.','Fallback form: retain the codes. Display conditions express routing: ∈ means “among”, ∉ “not among”, ∩ ≠ ∅ “contains at least one”. Do not answer fields whose conditions are not met. Partial responses are accepted. After refusing participation, stop without further answers.','Formulario alternativo: conserve los códigos. Las condiciones expresan los filtros: ∈ significa «entre», ∉ «no entre» y ∩ ≠ ∅ «contiene al menos uno». No responda si no se cumple la condición. Se aceptan respuestas parciales. Si rechaza participar, termine sin más respuestas.','Formulário alternativo: mantenha os códigos. As condições exprimem os filtros: ∈ significa «entre», ∉ «não entre» e ∩ ≠ ∅ «contém pelo menos um». Não responda se a condição não for cumprida. Aceitam-se respostas parciais. Se recusar participar, termine sem outras respostas.','نموذج بديل: حافظ على الرموز. تعبّر الشروط عن مسار الأسئلة: ∈ تعني «من بين»، و∉ «ليس من بين»، و∩ ≠ ∅ «يتضمن واحداً على الأقل». لا تجب عن الحقول التي لا تتحقق شروطها. الإجابات الجزئية مقبولة. عند رفض المشاركة، توقف دون إجابات أخرى.')
add('relay_resume','Reprendre le relevé CSV enregistré','Resume the saved CSV register','Reanudar el registro CSV guardado','Retomar o registo CSV guardado','استئناف سجل CSV المحفوظ')
add('relay_import','Importer le relevé','Import register','Importar registro','Importar registo','استيراد السجل')
add('relay_count','Invitations préparées ou déclarées envoyées — producteurs : {p} ; équipes utilisatrices : {u}. Ce ne sont pas des réponses reçues.','Invitations prepared or reported as sent — producers: {p}; user teams: {u}. These are not received responses.','Invitaciones preparadas o declaradas enviadas — productores: {p}; equipos usuarios: {u}. No son respuestas recibidas.','Convites preparados ou declarados enviados — produtores: {p}; equipas utilizadoras: {u}. Não são respostas recebidas.','الدعوات المعدّة أو المصرّح بإرسالها — المنتجون: {p}؛ فرق المستخدمين: {u}. هذه ليست إجابات مستلمة.')

# v2.1 : explicit submission checks and genuinely shrinking option lists.
add('add_choice','Ajouter une réponse','Add an answer','Añadir una respuesta','Adicionar uma resposta','إضافة إجابة')
add('choose_next','— Choisir une réponse à ajouter —','— Choose an answer to add —','— Elija una respuesta para añadir —','— Escolha uma resposta a adicionar —','— اختر إجابة لإضافتها —')
add('remove_choice','Retirer','Remove','Retirar','Retirar','إزالة')
add('multi_help','Choisissez les réponses une par une. Une réponse retenue disparaît de la liste disponible. « Retirer » la remet dans la liste.','Choose answers one at a time. A selected answer disappears from the available list. Remove restores it.','Elija las respuestas una a una. Una respuesta seleccionada desaparece de la lista disponible. Retirar la devuelve a la lista.','Escolha as respostas uma a uma. Uma resposta selecionada desaparece da lista disponível. Retirar repõe a opção.','اختر الإجابات واحدة تلو الأخرى. تختفي الإجابة المختارة من القائمة المتاحة، وتعيدها الإزالة إلى القائمة.')
add('multi_complete','Aucun autre choix compatible disponible. Retirez une réponse pour modifier la sélection.','No further compatible choices are available. Remove an answer to change your selection.','No quedan opciones compatibles. Retire una respuesta para modificar la selección.','Não há mais opções compatíveis. Retire uma resposta para alterar a seleção.','لا تتوفر اختيارات متوافقة أخرى. أزل إجابة لتعديل الاختيار.')
add('priority_help','Les choix des rangs précédents sont retirés de cette liste. Renseignez les priorités dans l’ordre ; une ou deux suffisent.','Choices at earlier ranks are removed from this list. Enter priorities in order; one or two are enough.','Las opciones de rangos anteriores se retiran de esta lista. Complete en orden; bastan una o dos prioridades.','As opções dos níveis anteriores são retiradas desta lista. Preencha por ordem; bastam uma ou duas prioridades.','تحذف اختيارات المراتب السابقة من هذه القائمة. أدخل الأولويات بالترتيب؛ تكفي أولوية أو أولويتان.')
add('need_confirmation','Cochez la confirmation de transmission située au-dessus du bouton.','Tick the submission confirmation above the button.','Marque la confirmación de envío situada sobre el botón.','Assinale a confirmação de envio acima do botão.','حدد تأكيد الإرسال الموجود أعلى الزر.')
add('need_authority','Confirmez l’autorisation institutionnelle, ou choisissez une restitution anonymisée dans la dernière rubrique.','Confirm institutional authorisation, or choose anonymised reporting in the last section.','Confirme la autorización institucional o elija una síntesis anonimizada en la última sección.','Confirme a autorização institucional ou escolha a síntese anonimizada na última secção.','أكد التصريح المؤسسي أو اختر عرضا دون تحديد الهوية في القسم الأخير.')
add('preview_block','L’application est en prévisualisation : aucun stockage n’est configuré. Contactez l’équipe ; ne ressaisissez pas tout le questionnaire.','The app is in preview: no storage is configured. Contact the team; do not re-enter the entire questionnaire.','La aplicación está en vista previa: no hay almacenamiento configurado. Contacte al equipo; no repita todo el cuestionario.','A aplicação está em pré-visualização: o armazenamento não está configurado. Contacte a equipa; não repita todo o questionário.','التطبيق في وضع المعاينة ولا يوجد تخزين مهيأ. تواصل مع الفريق ولا تعد تعبئة الاستبيان كاملا.')
add('need_access','Renseignez le code d’accès indiqué dans votre invitation.','Enter the access code provided in your invitation.','Introduzca el código de acceso de su invitación.','Introduza o código de acesso do convite.','أدخل رمز الوصول الوارد في الدعوة.')
add('mode_mismatch','Le mode de la campagne et celui de l’application diffèrent. L’équipe doit vérifier la configuration avant tout envoi.','The campaign and app modes differ. The team must check the configuration before submission.','El modo de la campaña y el de la aplicación difieren. El equipo debe revisar la configuración.','Os modos da campanha e da aplicação diferem. A equipa deve verificar a configuração.','يختلف وضع الحملة عن وضع التطبيق. يجب على الفريق مراجعة الإعدادات قبل الإرسال.')
add('checklist','Vérifications avant envoi','Checks before submission','Comprobaciones antes del envío','Verificações antes do envio','التحقق قبل الإرسال')
add('ready','Les conditions d’envoi sont réunies. Cliquez sur « Envoyer ma contribution » une seule fois.','The submission conditions are met. Click Submit my contribution once.','Se cumplen las condiciones. Pulse Enviar mi contribución una sola vez.','As condições estão reunidas. Clique em Enviar a minha contribuição uma única vez.','شروط الإرسال مستوفاة. اضغط على إرسال مساهمتي مرة واحدة.')
add('not_sent','La contribution n’a pas été envoyée. Corrigez les points indiqués ci-dessous.','The contribution was not sent. Address the points below.','La contribución no se ha enviado. Corrija los puntos siguientes.','A contribuição não foi enviada. Corrija os pontos abaixo.','لم ترسل المساهمة. صحح النقاط الموضحة أدناه.')
add('refresh','Actualiser l’état de la collecte','Refresh collection status','Actualizar el estado de la recogida','Atualizar o estado da recolha','تحديث حالة الجمع')
add('campaign_dates','Fenêtre configurée par l’équipe : {start} → {end} (UTC).','Collection window configured by the team: {start} → {end} (UTC).','Ventana configurada por el equipo: {start} → {end} (UTC).','Período configurado pela equipa: {start} → {end} (UTC).','الفترة التي حددها الفريق: {start} → {end} (UTC).')
add('previous_review','Précédent','Previous','Anterior','Anterior','السابق')
add('guide_fr','Guide détaillé de remplissage — français','Detailed completion guide — French','Guía detallada de cumplimentación — francés','Guia detalhado de preenchimento — francês','دليل التعبئة المفصل — بالفرنسية')
add('quick_help','Aide au remplissage','Completion help','Ayuda para cumplimentar','Ajuda ao preenchimento','المساعدة في التعبئة')
add('quick_help_text','1. Choisissez le questionnaire correspondant à votre fonction.\n2. Répondez sur les pratiques du 01/11/2025 au 31/10/2026.\n3. Utilisez Suivant et Précédent ; les choix déjà retenus ne sont pas proposés à nouveau.\n4. Téléchargez un brouillon avant de quitter. Le brouillon ne vaut pas envoi.\n5. Sur la page de relecture, confirmez la transmission puis cliquez sur Envoyer ma contribution. Seul un accusé confirme la réception.','1. Select the questionnaire matching your role.\n2. Describe practices from 1 Nov 2025 to 31 Oct 2026.\n3. Use Next and Previous; selected choices are not offered again.\n4. Download a draft before leaving. A draft is not a submission.\n5. On the review page, confirm and click Submit my contribution. Only a receipt confirms reception.','1. Elija el cuestionario correspondiente a su función.\n2. Describa prácticas del 01/11/2025 al 31/10/2026.\n3. Use Siguiente y Anterior; las opciones seleccionadas no se ofrecen de nuevo.\n4. Guarde un borrador antes de salir. No equivale al envío.\n5. En la revisión, confirme y pulse Enviar mi contribución. Solo un acuse confirma la recepción.','1. Escolha o questionário adequado à sua função.\n2. Descreva práticas de 01/11/2025 a 31/10/2026.\n3. Use Seguinte e Anterior; as escolhas selecionadas não voltam a ser propostas.\n4. Guarde um rascunho antes de sair. Não equivale ao envio.\n5. Na revisão, confirme e clique em Enviar a minha contribuição. Só o recibo confirma a receção.','1. اختر الاستبيان المناسب لوظيفتك.\n2. صف الممارسات من 01/11/2025 إلى 31/10/2026.\n3. استخدم التالي والسابق؛ لا تعرض الاختيارات المحددة مرة أخرى.\n4. نزل مسودة قبل المغادرة؛ المسودة ليست إرسالا.\n5. في صفحة المراجعة أكد الإرسال واضغط على إرسال مساهمتي. إشعار الاستلام وحده يؤكد الوصول.')
add('rejected_back','Revenir à la relecture — envoi refusé','Return to review — submission rejected','Volver a la revisión — envío rechazado','Voltar à revisão — envio recusado','العودة إلى المراجعة — رفض الإرسال')
add('error_code','Référence technique à communiquer à l’équipe : {code}. Ne communiquez aucun mot de passe.','Technical reference for the team: {code}. Do not share any password.','Referencia técnica para el equipo: {code}. No comparta contraseñas.','Referência técnica para a equipa: {code}. Não partilhe palavras-passe.','المرجع التقني للفريق: {code}. لا تشارك أي كلمة مرور.')


# --- core.py ---
# Documentation interne non affichee.
import json,hashlib,re,csv,io,zipfile,html,unicodedata,copy
from datetime import datetime,timezone
from uuid import UUID,uuid4
from collections import Counter
APP_VERSION='2.1.0'
SCHEMA_VERSION=SCHEMA['version']
FORMS=SCHEMA['forms']
LANGUAGES=SCHEMA['languages']
NOTICE=SCHEMA['notice']
MAX_DRAFT_BYTES=1000000
LANG_NAMES={'fr':'Français','en':'English','ar':'العربية','pt':'Português','es':'Español'}
class ValidationError(ValueError):pass

def text(value,lang='fr'):
    if not isinstance(value,dict):return str(value)
    if lang not in LANGUAGES:raise ValidationError('LANGUAGE')
    if lang not in value:raise ValidationError('MISSING_TRANSLATION')
    return value[lang]

def ui(key,lang='fr',**params):
    return UI[key][lang].format(**params)

def iter_fields(form_id):
    for section in FORMS[form_id]['sections']:
        for field in section['fields']:yield section,field

def blank(v):return v is None or v=='' or v==[]

def combine(vals,kind):
    if kind=='all':return False if False in vals else None if None in vals else True
    return True if True in vals else None if None in vals else False

def evaluate(rule,answers,states=None):
    if not rule:return True
    if 'all' in rule:return combine([evaluate(r,answers,states) for r in rule['all']],'all')
    if 'any' in rule:return combine([evaluate(r,answers,states) for r in rule['any']],'any')
    fid=rule['field'];op=rule['op'];v=answers.get(fid)
    if states and states.get(fid)=='NF':return False
    if states and states.get(fid)=='FILTRE_INDETERMINE':return None
    if op=='not_in':return v not in rule['values']
    if blank(v):return None
    if op=='in':return v in rule['values']
    if op=='contains':return isinstance(v,list) and any(x in v for x in rule['values'])
    raise ValidationError('UNKNOWN_FILTER')

def field_states(form_id,answers):
    known={f['id'] for _,f in iter_fields(form_id)}
    effective={k:v for k,v in answers.items() if k in known}
    states={}
    for s,f in iter_fields(form_id):
        fid=f['id'];active=combine([evaluate(s.get('gate'),effective,states),evaluate(f.get('gate'),effective,states)],'all')
        if active is not True:
            effective.pop(fid,None);states[fid]='NF' if active is False else 'FILTRE_INDETERMINE'
        else:
            v=effective.get(fid)
            states[fid]='NR_A_CONTROLER' if blank(v) else 'NSP' if v==98 or v==[98] else 'NA' if v==99 or v==[99] else 'RENSEIGNE'
    return effective,states

def active_sections(form_id,answers):
    a,_=field_states(form_id,answers)
    return [s for s in FORMS[form_id]['sections'] if evaluate(s.get('gate'),a) is not False]

def normalise_digits(value):
    return ''.join(str(unicodedata.decimal(c)) if c.isdecimal() else c for c in value)

def validation(form_id,answers,lang='fr',require_consent=True):
    if form_id not in FORMS or not isinstance(answers,dict):raise ValidationError('INVALID_FORM')
    effective,states=field_states(form_id,answers);errors=[];warnings=[]
    if require_consent and effective.get(FORMS[form_id]['consent'])!=1:errors.append(ui('no_consent',lang))
    if form_id=='producteurs' and effective.get('QP01_Produit')==0:errors.append(ui('ineligible',lang))
    for _,f in iter_fields(form_id):
        fid=f['id'];v=effective.get(fid)
        if blank(v):continue
        opts=[o['code'] for o in f['options']]
        if f['type']=='single':
            if isinstance(v,bool) or not isinstance(v,(str,int)) or v not in opts:errors.append(fid+' : '+ui('bad_code',lang))
        elif f['type']=='multi':
            if not isinstance(v,list) or any(isinstance(n,bool) or not isinstance(n,(str,int)) or n not in opts for n in v):errors.append(fid+' : '+ui('bad_selection',lang));continue
            if len(v)!=len(set(v)):errors.append(fid+' : '+ui('bad_selection',lang))
            if len(v)>f.get('max_choices',len(opts)):errors.append(fid+' : '+ui('too_many',lang))
            if len(v)>1 and any(n in f.get('exclusive',[]) for n in v):errors.append(fid+' : '+ui('exclusive',lang))
        else:
            if not isinstance(v,str) or len(v)>f.get('max_chars',2500) or '\x00' in v:errors.append(fid+' : '+ui('text_required',lang));continue
            if f.get('max_words') and len(v.split())>f['max_words']:warnings.append(fid+' : '+ui('word_warning',lang))
            if f['type']=='email' and not re.fullmatch(r'[^\s@]+@[^\s@]+\.[^\s@]+',v):errors.append(fid+' : '+ui('email_error',lang))
            if f['type']=='month':
                m=re.fullmatch(r'(0[1-9]|1[0-2])/(\d{4})',normalise_digits(v))
                if not m:errors.append(fid+' : '+ui('month_error',lang))
                elif not 202511<=int(m[2])*100+int(m[1])<=202610:warnings.append(fid+' : '+ui('month_warning',lang))
    prefix=FORMS[form_id].get('priority_prefix')
    if prefix:
        vals=[effective.get(prefix+f'_P{i}') for i in (1,2,3)];valid=[v for v in vals if not blank(v)]
        if all(isinstance(v,int) for v in valid):
            if len(valid)!=len(set(valid)) or (not blank(vals[1]) and blank(vals[0])) or (not blank(vals[2]) and blank(vals[1])):errors.append(ui('priority_error',lang))
    if form_id in ('ins','producteurs'):
        p='QI' if form_id=='ins' else 'QP'
        if effective.get(p+'02a')==0 and effective.get(p+'02b')==1:errors.append(ui('volume_error',lang))
        if effective.get(p+'04a')==5 and effective.get(p+'04b') in (0,1):warnings.append(ui('deadline_warning',lang))
    if form_id=='utilisateurs':
        if effective.get('QU02a')==0 and effective.get('QU02b')==1:errors.append(ui('support_error',lang))
        d=effective.get('QU03_Delai');out=effective.get('QU03_Issue')
        if (d==99 and out in (1,2,3,4)) or (out==99 and d in (1,2,3,4,5)):errors.append(ui('support_error',lang))
    known={f['id'] for _,f in iter_fields(form_id)}
    for _,f in iter_fields(form_id):
        v=effective.get(f['id']);other=f['id'].rsplit('_P',1)[0]+'_Autre' if re.fullmatch(r'(?:QI10|QP10|QU07)_P[123]',f['id']) else f['id']+'_Autre'
        if other in known and (v==97 or isinstance(v,list) and 97 in v) and blank(effective.get(other)):warnings.append(f['id']+' : '+ui('other_warning',lang))
    return list(dict.fromkeys(errors)),list(dict.fromkeys(warnings))

def schema_hash(form_id):return hashlib.sha256(json.dumps(FORMS[form_id],ensure_ascii=False,sort_keys=True).encode()).hexdigest()

def prepare_submission(form_id,answers,lang='fr',submission_id=None,is_test=False,metadata=None):
    if form_id not in FORMS or lang not in LANGUAGES or type(is_test) is not bool:raise ValidationError('INVALID_FORM')
    errors,warnings=validation(form_id,answers,lang)
    if errors:raise ValidationError('\n'.join(errors))
    effective,states=field_states(form_id,answers);fields={f['id']:f for _,f in iter_fields(form_id)}
    contacts={k:v for k,v in effective.items() if fields[k].get('private') and not blank(v)}
    clean={k:v for k,v in effective.items() if not fields[k].get('private') and not blank(v)}
    if not any(k!=FORMS[form_id]['consent'] for k in clean):raise ValidationError(ui('content_required',lang))
    try:uid=str(UUID(submission_id)) if submission_id else str(uuid4())
    except (ValueError,TypeError,AttributeError) as exc:raise ValidationError('INVALID_ID') from exc
    attr=clean.get(FORMS[form_id]['attribution']);meta=metadata or {}
    meta={k:str(v)[:120] for k,v in meta.items() if k in ('relay_id','invitation_id','channel') and v}
    payload={'submission_id':uid,'form_id':form_id,'language':lang,'schema_version':SCHEMA_VERSION,'instrument_version':FORMS[form_id]['version'],'translation_version':'2026-11-16.v2','instrument_hash':schema_hash(form_id),'app_version':APP_VERSION,'is_test':is_test,'answers':clean,'field_states':{k:v for k,v in states.items() if not fields[k].get('private')},'citation_permission':0 if blank(attr) else attr,'citation_explicit':not blank(attr),'warnings':warnings,'short_form':form_id=='abrege','metadata':meta,'response_language':clean.get(FORMS[form_id].get('response_language','')) or 'non_declared','intended_deliverable':'L3'}
    return payload,contacts

def draft_bytes(form_id,answers,lang,submission_id,metadata=None):
    effective,_=field_states(form_id,answers)
    return json.dumps({'kind':'hiswaca_draft','schema_version':SCHEMA_VERSION,'form_id':form_id,'language':lang,'submission_id':str(UUID(submission_id)),'answers':effective,'metadata':metadata or {}},ensure_ascii=False,indent=2).encode('utf-8')

def load_draft(content):
    if len(content)>MAX_DRAFT_BYTES:raise ValidationError('INVALID_FILE')
    try:
        d=json.loads(content.decode('utf-8-sig'))
        if not isinstance(d,dict) or d.get('kind')!='hiswaca_draft' or d.get('schema_version')!=SCHEMA_VERSION or d.get('language') not in LANGUAGES or d.get('form_id') not in FORMS or not isinstance(d.get('answers'),dict):raise ValueError()
        d['submission_id']=str(UUID(d['submission_id']))
        known={f['id'] for _,f in iter_fields(d['form_id'])}
        if set(d['answers'])-known:raise ValueError()
        errors,_=validation(d['form_id'],d['answers'],d['language'],False)
        if errors:raise ValueError()
        d['answers'],_=field_states(d['form_id'],d['answers'])
        d['metadata']={k:str(v)[:120] for k,v in (d.get('metadata') or {}).items() if k in ('relay_id','invitation_id','channel')}
        return d
    except (ValueError,TypeError,KeyError,UnicodeDecodeError,AttributeError) as exc:raise ValidationError('INVALID_FILE') from exc

def load_pending(content,test_mode):
    if len(content)>MAX_DRAFT_BYTES:raise ValidationError('INVALID_FILE')
    try:
        d=json.loads(content.decode('utf-8-sig'));p=d['payload'];c=d['contacts']
        if d.get('kind')!='hiswaca_pending' or p['schema_version']!=SCHEMA_VERSION or p['is_test'] is not test_mode:raise ValueError()
        a={**p['answers'],**c};clean,contacts=prepare_submission(p['form_id'],a,p['language'],p['submission_id'],test_mode,p.get('metadata'))
        if any(clean.get(k)!=p.get(k) for k in ('answers','field_states','citation_permission','citation_explicit','instrument_hash','metadata','intended_deliverable')) or contacts!=c:raise ValueError()
        if p.get('citation_permission') in (1,2) and p.get('attribution_authority_confirmed') is not True:raise ValueError()
        return {'payload':p,'contacts':c}
    except (ValueError,TypeError,KeyError,UnicodeDecodeError,AttributeError) as exc:raise ValidationError('INVALID_FILE') from exc

def answer_label(field,value,lang='fr'):
    opts={o['code']:text(o['label'],lang) for o in field['options']}
    if blank(value):return ''
    if isinstance(value,list):return ' ; '.join(str(n)+' — '+opts.get(n,str(n)) for n in value)
    if field['type']=='single':return str(value)+' — '+opts.get(value,str(value))
    return str(value)

def receipt_text(payload,receipt):
    lang=payload['language'];lines=['HISWACA — '+ui('receipt_title',lang),ui('reference',lang)+' : '+receipt['receipt'],'UTC : '+receipt['received_at'],ui('version',lang)+' : '+payload['schema_version']]
    if payload['is_test']:lines += [ui('test',lang)]
    for _,f in iter_fields(payload['form_id']):
        if not f.get('private') and not blank(payload['answers'].get(f['id'])):lines.append(f['id']+' — '+text(f['label'],lang)+'\n'+answer_label(f,payload['answers'][f['id']],lang))
    return '\n\n'.join(lines+[ui('not_validation',lang)])

def csv_safe(v):
    if v is None:return ''
    s=str(v)
    return "'"+s if s.lstrip().startswith(('=','+','-','@')) or s.startswith(('\t','\r','\n')) else s

def csv_bytes(rows,columns):
    out=io.StringIO(newline='');w=csv.DictWriter(out,fieldnames=columns,delimiter=';',lineterminator='\r\n',extrasaction='ignore');w.writeheader()
    for r in rows:w.writerow({k:csv_safe(r.get(k,'')) for k in columns})
    return out.getvalue().encode('utf-8-sig')

def collection_wave(timestamp):
    try:
        dt=datetime.fromisoformat(timestamp.replace('Z','+00:00'))
        if dt.tzinfo is None:return 'Date à vérifier'
        start=datetime(2026,11,19,tzinfo=timezone.utc);end=datetime(2026,11,27,tzinfo=timezone.utc)
        return 'Avant ouverture' if dt<start else 'Vague principale' if dt<end else 'Tardive / complément'
    except (ValueError,TypeError,AttributeError):return 'Date à vérifier'

def contribution_rows(records):
    out=[]
    for r in records:
        p=r['payload'];f=FORMS.get(p.get('form_id'),{});a=p.get('answers',{});review=r.get('review') or {};m=p.get('metadata') or {}
        institution=a.get(f.get('institution',''),'');country=a.get(f.get('country',''),'');status=a.get(f.get('status',''),'')
        out.append({'ID réponse':r['receipt'],'Institution':institution,'Pays / zone':country,'Profil':p['form_id'],'Profil d’origine abrégé':a.get('RA01_Profil','') if p['form_id']=='abrege' else '', 'Statut contribution':status,'Unité':a.get(f.get('unit',''),''),'Domaine':a.get(f.get('sector',''),''),'Langue interface':p.get('language'),'Langue texte':p.get('response_language','non_declared'),'INS relais':a.get(f.get('relay',''),''),'ID relais':m.get('relay_id',''),'ID invitation':m.get('invitation_id',''),'Origine invitation':a.get(f.get('origin',''),''),'Date réponse':r['received_at'],'Vague':collection_wave(r['received_at']),'Exploitabilité':review.get('status','À examiner'),'Lié à':review.get('linked_to',''),'Droits de citation':p.get('citation_permission',0),'Permission explicite':p.get('citation_explicit',False),'Test':p.get('is_test',True),'Version':p.get('schema_version',''),'Formulaire abrégé':p.get('short_form',False),'Champs actifs non renseignés':sum(v=='NR_A_CONTROLER' for v in p.get('field_states',{}).values()),'Filtres indéterminés':sum(v=='FILTRE_INDETERMINE' for v in p.get('field_states',{}).values()),'Destination':'L3','Ancienne version':p.get('schema_version')!=SCHEMA_VERSION})
    return out

def response_rows(records):
    rows=[]
    for r in records:
        p=r['payload'];f_id=p['form_id'];known={f['id']:f for _,f in iter_fields(f_id)} if f_id in FORMS and p.get('schema_version')==SCHEMA_VERSION else {};a=p.get('answers',{});states=p.get('field_states',{})
        for fid in dict.fromkeys([*known,*a]):
            f=known.get(fid,{});v=a.get(fid)
            if f.get('private'):continue
            vals=v if isinstance(v,list) and v else [v]
            for rank,val in enumerate(vals,1):
                rows.append({'ID réponse':r['receipt'],'Profil':f_id,'Code question':fid,'Libellé FR':text(f['label']) if f else 'Ancien champ : consulter le dictionnaire de sa version','Réponse brute':json.dumps(v,ensure_ascii=False) if isinstance(v,list) else v,'Code option':val if isinstance(v,list) else '', 'Rang option':rank if isinstance(v,list) else '', 'Code analytique':val if f.get('type') in ('single','multi') else '', 'Statut information':states.get(fid,'NON_PRESENT_VERSION'),'Traduction FR':'','Statut traduction':'À contrôler' if p.get('language')!='fr' and f.get('type') in ('text','textarea') and not blank(v) else '', 'Langue interface':p.get('language'),'Version':p.get('schema_version'),'Test':p.get('is_test',True),'Destination':'L3','Référence précise':f.get('source','') if p.get('schema_version')==SCHEMA_VERSION else 'Consulter le dictionnaire de la version originale'})
    return rows

def dictionary_rows():
    rows=[]
    for fid,form in FORMS.items():
        for s,f in iter_fields(fid):
            for opt in f['options'] or [{'code':'','label':dict.fromkeys(LANGUAGES,'')}]:
                row={'Parcours':fid,'Rubrique':s['id'],'Champ':f['id'],'Type':f['type'],'Privé':bool(f.get('private')),'Code':opt['code'],'Filtre':json.dumps({'section':s.get('gate'),'champ':f.get('gate')},ensure_ascii=False),'Max choix':f.get('max_choices',''),'Mots indicatifs':f.get('max_words',''),'Source':f.get('source',s['source']),'Version':SCHEMA_VERSION}
                for l in LANGUAGES:row['Question '+l]=text(f['label'],l);row['Modalité '+l]=text(opt['label'],l)
                rows.append(row)
    return rows

def export_bundle(records,registries=None):
    buf=io.BytesIO()
    with zipfile.ZipFile(buf,'w',zipfile.ZIP_DEFLATED) as z:
        for name,rows in [('Contributions',contribution_rows(records)),('Reponses',response_rows(records)),('Dictionnaire',dictionary_rows())]:
            columns=list(rows[0]) if rows else ['Aucun enregistrement'];z.writestr(name+'.csv',csv_bytes(rows,columns))
        for r in records:z.writestr('originaux/'+r['id']+'.json',json.dumps(r,ensure_ascii=False,indent=2))
        if registries:
            for kind,rows in registries.items():
                if not rows:continue
                clean=[{**r.get('data',{}),'_record_id':r['record_id'],'_version':r['version'],'_is_test':r['is_test']} for r in rows]
                cols=list(dict.fromkeys(k for r in clean for k in r));z.writestr('registres/'+kind+'.csv',csv_bytes(clean,cols))
        z.writestr('LISEZ_MOI.txt','EXPORT INTERNE RESTREINT. Les contacts structurés sont séparés, mais les récits et noms d’institutions peuvent identifier les contributeurs. Ne pas publier sans revue des autorisations.\nLes JSON sont les originaux. Les formules potentielles sont neutralisées dans les CSV. NR_A_CONTROLER n’est pas NR confirmé; FILTRE_INDETERMINE n’est pas NF. Les classes ne sont pas des quantités.\nExclure les tests, doublons et hors champ des analyses. Ne pas additionner les demandes de plusieurs producteurs en total national. Aucune extrapolation ni score de maturité. Données de consultation uniquement pour L3. Les anciennes versions restent identifiées et ne sont pas automatiquement converties.\nLes traductions et les décisions de codage sont ajoutées dans le registre dédié sans remplacer l’original.\n')
    return buf.getvalue()

def rule_text(rule,lang):
    if not rule:return ''
    if 'all' in rule:return '('+(' '+ui('logic_and',lang)+' ').join(rule_text(x,lang) for x in rule['all'])+')'
    if 'any' in rule:return '('+(' '+ui('logic_or',lang)+' ').join(rule_text(x,lang) for x in rule['any'])+')'
    symbols={'in':'∈','not_in':'∉','contains':'∩ ≠ ∅'}
    return rule['field']+' '+symbols[rule['op']]+' {'+', '.join(str(x) for x in rule['values'])+'}'

def fallback_text(form_id,lang):
    f=FORMS[form_id];lines=[text(f['title'],lang),SCHEMA_VERSION,ui('dates',lang),text(NOTICE,lang),ui('fallback_filter_guide',lang)]
    for s in f['sections']:
        lines.extend(['\n'+s['id']+' — '+text(s['title'],lang),text(s['note'],lang)])
        for field in s['fields']:
            lines.append(field['id']+' — '+text(field['label'],lang))
            if s.get('gate') or field.get('gate'):
                lines.append(ui('condition',lang)+' : '+rule_text({'all':[x for x in [s.get('gate'),field.get('gate')] if x]},lang))
            if field.get('max_choices'):lines.append(ui('max_choices',lang,n=field['max_choices']))
            if field.get('max_words'):lines.append(ui('words',lang,n=field['max_words']))
            lines.extend('[ ] '+str(o['code'])+' '+text(o['label'],lang) for o in field['options'])
            lines.append('____________________________')
    lines.append(ui('not_validation',lang))
    return '\n'.join(lines).encode('utf-8-sig')


# Interface helpers — values remain the original instrument codes.
def remaining_choices(field, selected):
    selected = list(dict.fromkeys(selected or []))
    codes = [o['code'] for o in field.get('options', [])]
    if len(selected) >= field.get('max_choices', len(codes)):
        return []
    exclusive = field.get('exclusive', [])
    if any(v in exclusive for v in selected):
        return []
    return [c for c in codes if c not in selected and (not selected or c not in exclusive)]


def priority_choices(form_id, field, answers):
    prefix = FORMS[form_id].get('priority_prefix')
    fid = field['id']
    codes = [o['code'] for o in field.get('options', [])]
    if not prefix or fid not in [prefix + '_P' + str(i) for i in (1, 2, 3)]:
        return codes
    rank = int(fid[-1])
    used = [answers.get(prefix + '_P' + str(i)) for i in range(1, rank)]
    if rank > 1 and (any(blank(v) for v in used) or used[0] in (0, 98)):
        return []
    return [c for c in codes if c not in used]


def submission_requirements(form_id, answers, lang, settings, info,
                            confirmed, authority, access_code='', storage_error=''):
    """Return explicit blocking reasons; never bypass campaign or consent rules."""
    errors, _ = validation(form_id, answers, lang)
    effective, _ = field_states(form_id, answers)
    substantive = [f for _, f in iter_fields(form_id)
                   if f['id'] != FORMS[form_id]['consent'] and not f.get('private')]
    if not any(not blank(effective.get(f['id'])) for f in substantive):
        errors.append(ui('content_required', lang))
    if not confirmed:
        errors.append(ui('need_confirmation', lang))
    if effective.get(FORMS[form_id]['attribution']) in (1, 2) and not authority:
        errors.append(ui('need_authority', lang))
    if settings.mode == 'preview':
        errors.append(ui('preview_block', lang))
    elif storage_error or not isinstance(info, dict):
        errors.append(ui('service_error', lang))
    else:
        if info.get('active') is not True:
            errors.append(ui('closed', lang))
        if info.get('requires_access_code') and not str(access_code).strip():
            errors.append(ui('need_access', lang))
        if type(info.get('test_mode')) is bool and info['test_mode'] != settings.test_mode:
            errors.append(ui('mode_mismatch', lang))
    if settings.mode == 'production' and not settings.test_mode and lang not in settings.approved_languages:
        errors.append(ui('unreviewed', lang))
    return list(dict.fromkeys(errors))


# --- storage.py ---
# Documentation interne non affichee.
import hashlib
import json
import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime,timezone
from pathlib import Path
from typing import Any
from uuid import UUID,uuid4
import requests


class StorageError(RuntimeError):
    pass


class ConfigurationError(ValueError):
    pass


def utcnow():
    return datetime.now(timezone.utc).isoformat()


def canonical(value):
    return json.dumps(value,sort_keys=True,ensure_ascii=False,separators=(",",":"))


def digest(payload,contacts):
    return hashlib.sha256(canonical([payload,contacts]).encode()).hexdigest()


@dataclass
class Settings:
    mode: str="preview"
    url: str=""
    public_key: str=""
    admin_key: str=""
    writer_token: str=""
    campaign: str="discours-2026-v2"
    test_mode: bool=True
    contact_email: str=""
    notice_controller: str="STATAFRIC — consultant et personnes désignées pour cette mission"
    retention_notice: str="Durée de conservation et hébergement à confirmer avant ouverture."
    collection_url: str=""
    local_path: str=".local/diagnostic_v2_test.sqlite3"
    approved_languages: tuple=()
    published_forms: tuple=("ins","producteurs","utilisateurs","partenaires","abrege")

    @classmethod
    def from_mapping(cls, config, admin=False):
        obj=cls(mode=str(config.get("APP_MODE","preview")),url=str(config.get("SUPABASE_URL","")),
                public_key=str(config.get("SUPABASE_PUBLIC_KEY","")),admin_key=str(config.get("SUPABASE_ADMIN_KEY","")),
                writer_token=str(config.get("WRITER_TOKEN","")),campaign=str(config.get("CAMPAIGN_ID","discours-2026-v2")),
                test_mode=config.get("TEST_MODE",True),contact_email=str(config.get("CONTACT_EMAIL","")),
                notice_controller=str(config.get("NOTICE_CONTROLLER",cls.notice_controller)),
                retention_notice=str(config.get("RETENTION_NOTICE",cls.retention_notice)),
                collection_url=str(config.get("COLLECTION_URL","")),local_path=str(config.get("LOCAL_DB_PATH",cls.local_path)))
        if type(obj.test_mode) is not bool:
            raise ConfigurationError("TEST_MODE doit être un booléen TOML true/false, sans guillemets.")
        approved=config.get("APPROVED_LANGUAGES",[])
        if not isinstance(approved,list) or any(x not in ("fr","en","ar","pt","es") for x in approved):
            raise ConfigurationError("APPROVED_LANGUAGES doit être une liste de codes fr/en/ar/pt/es.")
        obj.approved_languages=tuple(approved)
        if obj.mode not in ["preview","local","production"]:
            raise ConfigurationError("APP_MODE doit être preview, local ou production.")
        if obj.mode=="production":
            from urllib.parse import urlparse
            parsed=urlparse(obj.url)
            # Service choisi pour cette version ; pas de point de sortie arbitraire.
            if parsed.scheme!="https" or not parsed.hostname or not parsed.hostname.endswith(".supabase.co") or parsed.path not in ["","/"] or parsed.query or parsed.fragment or parsed.username:
                raise ConfigurationError("SUPABASE_URL doit être l’URL HTTPS du projet Supabase (*.supabase.co).")
            if admin:
                if not obj.admin_key:
                    raise ConfigurationError("Configurer SUPABASE_ADMIN_KEY uniquement dans l’application d’administration.")
            else:
                if obj.admin_key:
                    raise ConfigurationError("Retirer SUPABASE_ADMIN_KEY des secrets de l’application publique.")
                if not obj.public_key or len(obj.writer_token)<32:
                    raise ConfigurationError("Configurer la clé publique Supabase et un WRITER_TOKEN d’au moins 32 caractères.")
                if not obj.contact_email or config.get("DEPLOYMENT_APPROVED") is not True or not config.get("RETENTION_NOTICE"):
                    raise ConfigurationError("Renseigner CONTACT_EMAIL et RETENTION_NOTICE puis confirmer DEPLOYMENT_APPROVED avant la collecte réelle.")
                if not obj.test_mode:
                    from collections.abc import Mapping
                    notices=config.get("notice",{})
                    if not obj.approved_languages or not isinstance(notices,Mapping) or any(not str(notices.get(l,'')).strip() for l in obj.approved_languages):
                        raise ConfigurationError("Configurer une notice d’hébergement et de conservation relue pour chaque langue autorisée dans [notice].")

        if obj.mode=="local":
            obj.test_mode=True
        return obj


class LocalStore:
    """Tests locaux uniquement : toutes les réponses sont marquées is_test."""
    def __init__(self,path):
        self.path=str(path)
        Path(path).parent.mkdir(parents=True,exist_ok=True)
        with self._db() as db:
            db.executescript("""
                PRAGMA journal_mode=WAL;
                CREATE TABLE IF NOT EXISTS responses (
                  id TEXT PRIMARY KEY, receipt TEXT UNIQUE NOT NULL, received_at TEXT NOT NULL,
                  content_hash TEXT NOT NULL, payload TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS contacts (
                  response_id TEXT PRIMARY KEY REFERENCES responses(id) ON DELETE CASCADE, contact TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS registry_events (
                  id TEXT PRIMARY KEY, record_id TEXT NOT NULL, kind TEXT NOT NULL,
                  version INTEGER NOT NULL, created_at TEXT NOT NULL, actor TEXT NOT NULL,
                  is_test INTEGER NOT NULL, data TEXT NOT NULL,
                  UNIQUE(record_id, version));
                CREATE TABLE IF NOT EXISTS reviews (
                  id TEXT PRIMARY KEY, response_id TEXT NOT NULL REFERENCES responses(id),
                  created_at TEXT NOT NULL, actor TEXT NOT NULL, status TEXT NOT NULL,
                  note TEXT NOT NULL, linked_to TEXT NOT NULL);
            """)
    @contextmanager
    def _db(self):
        db=sqlite3.connect(self.path,timeout=10)
        db.row_factory=sqlite3.Row
        db.execute("PRAGMA foreign_keys=ON")
        try:
            with db:
                yield db
        finally:
            db.close()
    def campaign_info(self):
        return {"active":True,"requires_access_code":False,"opens_at":None,"closes_at":None,"mode":"local"}
    def submit(self,payload,contacts,access_code=""):
        if payload.get("is_test") is not True:
            raise StorageError("Le stockage local est réservé aux réponses de test.")
        if payload.get("citation_permission") in [1,2] and payload.get("attribution_authority_confirmed") is not True:
            raise StorageError("INVALID_SUBMISSION")
        uid=str(UUID(payload["submission_id"]))
        h=digest(payload,contacts)
        try:
            with self._db() as db:
                db.execute("BEGIN IMMEDIATE")
                row=db.execute("SELECT * FROM responses WHERE id=?",(uid,)).fetchone()
                if row:
                    if row["content_hash"]!=h:
                        raise StorageError("Identifiant déjà utilisé avec un contenu différent. Ne pas écraser la contribution initiale.")
                    return {"receipt":row["receipt"],"received_at":row["received_at"],"duplicate":True}
                receipt="REP-"+uid
                date=utcnow()
                db.execute("INSERT INTO responses VALUES (?,?,?,?,?)",(uid,receipt,date,h,canonical(payload)))
                if contacts:
                    db.execute("INSERT INTO contacts VALUES (?,?)",(uid,canonical(contacts)))
                return {"receipt":receipt,"received_at":date,"duplicate":False}
        except sqlite3.Error as exc:
            raise StorageError("Échec du stockage local ; aucun accusé de réception fiable n’est disponible.") from exc
    def records(self):
        with self._db() as db:
            rows=db.execute("SELECT * FROM responses ORDER BY received_at,id").fetchall()
            return [{"id":r["id"],"receipt":r["receipt"],"received_at":r["received_at"],"payload":json.loads(r["payload"]),
                     "review":self.latest_review(r["id"])} for r in rows]
    def latest_review(self,uid):
        with self._db() as db:
            r=db.execute("SELECT * FROM reviews WHERE response_id=? ORDER BY created_at DESC,id DESC LIMIT 1",(uid,)).fetchone()
            return dict(r) if r else None
    def all_contacts(self):
        with self._db() as db:
            return [{"response_id":r["response_id"],"contact":json.loads(r["contact"])} for r in db.execute("SELECT * FROM contacts")]
    def add_review(self,uid,actor,status,note,linked_to=""):
        with self._db() as db:
            db.execute("INSERT INTO reviews VALUES (?,?,?,?,?,?,?)",(str(uuid4()),uid,utcnow(),actor,status,note,linked_to))
    def reviews(self):
        with self._db() as db:
            return [dict(r) for r in db.execute("SELECT * FROM reviews ORDER BY created_at,id")]

    def registry_events(self,kind=None):
        with self._db() as db:
            rows=db.execute("SELECT * FROM registry_events"+(" WHERE kind=?" if kind else "")+" ORDER BY created_at,id",(kind,) if kind else ()).fetchall()
            return [{**dict(r),"is_test":bool(r['is_test']),"data":json.loads(r['data'])} for r in rows]
    def save_registry(self,kind,record_id,data,actor,expected_version=0,is_test=True):
        if not is_test:raise StorageError("Le stockage local est réservé aux tests.")
        with self._db() as db:
            db.execute("BEGIN IMMEDIATE")
            row=db.execute("SELECT MAX(version) AS v FROM registry_events WHERE record_id=?",(record_id,)).fetchone()
            version=row['v'] or 0
            if version!=expected_version:raise StorageError("VERSION_CONFLICT")
            event_id=str(uuid4());created=utcnow()
            db.execute("INSERT INTO registry_events VALUES (?,?,?,?,?,?,?,?)",(event_id,record_id,kind,version+1,created,actor,1,canonical(data)))
            return {"id":event_id,"record_id":record_id,"version":version+1,"created_at":created}


class SupabaseStore:
    def __init__(self,settings: Settings,admin=False):
        self.s=settings
        self.admin=admin
        self.key=settings.admin_key if admin else settings.public_key
    def _request(self,method,path,body=None,params=None):
        headers={"apikey":self.key,"Content-Type":"application/json","Prefer":"return=representation"}
        if self.key.startswith("eyJ"):
            headers["Authorization"]="Bearer "+self.key
        try:
            response=requests.request(method,self.s.url.rstrip("/")+"/rest/v1/"+path,
                headers=headers,json=body,params=params,timeout=(5,25),allow_redirects=False)
        except requests.RequestException as exc:
            raise StorageError("NETWORK_UNCERTAIN") from exc
        if not 200<=response.status_code<300:
            # Ne jamais afficher une réponse brute pouvant inclure des détails SQL.
            message=""
            try:
                error_body=response.json()
                message=str(error_body.get("message","")) if isinstance(error_body,dict) else ""
            except ValueError: pass
            known=["COLLECTION_CLOSED","INVALID_ACCESS_CODE","SUBMISSION_CONFLICT","QUOTA_REACHED","INVALID_SUBMISSION","INVALID_WRITER","VERSION_CONFLICT","TEST_MODE_MISMATCH"]
            safe=next((code for code in known if code in message),"REMOTE_ERROR")
            raise StorageError(safe)
        try:
            return response.json() if response.content else None
        except ValueError as exc:
            raise StorageError("REMOTE_ERROR") from exc
    def campaign_info(self):
        result=self._request("POST","rpc/diag_campaign_info",{"p_campaign":self.s.campaign,"p_writer_token":self.s.writer_token})
        if not isinstance(result,dict) or type(result.get("active")) is not bool:
            raise StorageError("REMOTE_ERROR")
        return result
    def submit(self,payload,contacts,access_code=""):
        result=self._request("POST","rpc/diag_submit",{"p_campaign":self.s.campaign,"p_writer_token":self.s.writer_token,
                    "p_access_code":access_code,"p_payload":payload,"p_contacts":contacts})
        if not isinstance(result,dict) or result.get("receipt")!="REP-"+payload["submission_id"] or not isinstance(result.get("received_at"),str):
            raise StorageError("NETWORK_UNCERTAIN")
        try:
            datetime.fromisoformat(result["received_at"].replace("Z","+00:00"))
        except ValueError as exc:
            raise StorageError("NETWORK_UNCERTAIN") from exc
        return result
    def _all(self,table,select="*",extra=None):
        if not self.admin:
            raise StorageError("L’application publique ne peut pas lire les réponses.")
        cutoff=utcnow()
        out=[]; offset=0
        while True:
            params={"select":select,"limit":500,"offset":offset,"order":"created_at.asc,id.asc", "created_at":"lte."+cutoff,**(extra or {})}
            rows=self._request("GET",table,params=params)
            if not isinstance(rows,list):
                raise StorageError("REMOTE_ERROR")
            out.extend(rows)
            if len(rows)<500: break
            offset+=500
            if offset>50_000:
                raise StorageError("Export trop volumineux : utiliser un export administrateur de la base.")
        return out
    def records(self):
        rows=self._all("diag_responses",extra={"campaign_id":"eq."+self.s.campaign})
        review_rows=self.reviews()
        latest={}
        for r in review_rows: latest[r["response_id"]]=r
        return [{"id":r["id"],"receipt":r["receipt"],"received_at":r["created_at"],"payload":r["payload"],"review":latest.get(r["id"])} for r in rows]
    def reviews(self):
        return self._all("diag_reviews",extra={"campaign_id":"eq."+self.s.campaign})
    def all_contacts(self):
        return self._all("diag_contacts",select="id,response_id,contact,created_at",extra={"campaign_id":"eq."+self.s.campaign})
    def add_review(self,uid,actor,status,note,linked_to=""):
        if not self.admin: raise StorageError("Accès refusé.")
        return self._request("POST","diag_reviews",{"id":str(uuid4()),"campaign_id":self.s.campaign,"response_id":uid,
                        "actor":actor,"status":status,"note":note,"linked_to":linked_to or None})
    def registry_events(self,kind=None):
        extra={"campaign_id":"eq."+self.s.campaign}
        if kind:extra["kind"]="eq."+kind
        return self._all("diag_registry_events",extra=extra)
    def save_registry(self,kind,record_id,data,actor,expected_version=0,is_test=True):
        if not self.admin:raise StorageError("Accès refusé.")
        return self._request("POST","rpc/diag_registry_save",{"p_campaign":self.s.campaign,"p_kind":kind,
           "p_record_id":record_id,"p_expected_version":expected_version,"p_data":data,"p_actor":actor,"p_is_test":is_test})
    def set_active(self,active: bool):
        if not self.admin: raise StorageError("Accès refusé.")
        return self._request("PATCH","diag_campaigns",{"active":active},params={"id":"eq."+self.s.campaign})
    def admin_campaign(self):
        if not self.admin: raise StorageError("Accès refusé.")
        rows=self._request("GET","diag_campaigns",params={"id":"eq."+self.s.campaign,"select":"id,active,opens_at,closes_at,max_responses,test_mode"})
        return rows[0] if rows else None


def make_store(settings: Settings,admin=False):
    if settings.mode=="production": return SupabaseStore(settings,admin)
    if settings.mode=="local": return LocalStore(settings.local_path)
    return None


# --- registers.py ---
# Documentation interne non affichee.
REGISTERS={
 'invitations':{'title':'Invitations et relais','columns':{
  'Pays':'text','INS relais':'text','ID relais':'text','ID invitation':'text','Institution':'text','Service':'text','Domaine':'text',
  'Profil':['ins','producteurs','utilisateurs','partenaires','abrege'],'Langue':['fr','en','ar','pt','es','Non connue'],
  'Sélection':['Directe','Relais INS','Spontanée'],'Motif de sélection':'long','Canal':['Courriel','Courrier','Téléphone','Autre'],
  'Date invitation':'date','Échéance':'date','Relance 1':'date','Relance 2':'date','Statut':['Préparé','Envoyé','Non remis','Relancé','Réponse reçue','Refus'],'ID réponse':'text','Limite ou adaptation':'long'}},
 'documents':{'title':'Documents et recherches multilingues','columns':{
  'ID document':'text','Titre original':'long','Institution':'text','Pays / périmètre':'text','Type':['Norme','Guide','Procédure','Publication statistique','Discours','Décision','Autre'],
  'Langue':['fr','en','ar','pt','es','sw','Autre'],'Date publication':'date','Version source':'text','Lien public ou emplacement autorisé':'text','Date consultation':'date','Pages ou passage':'long','Extrait original':'long','Traduction FR':'long',
  'Statut traduction':['Non nécessaire','À relire','Relue','Ambiguïté non résolue'],'Source effectivement lue':['Texte complet','Passage ciblé','Notice seulement','Non accessible'],
  'Requête et site exploré':'long','Méthode / relecteur traduction':'text','Décision':['À examiner','Retenu','Écarté','À compléter'],'Motif / limite':'long','ID famille multilingue':'text','Destination':['L2','Complément documentaire L3'],'Version livrable':'text'}},
 'discours':{'title':'Évaluation des extraits de discours','columns':{
  'ID discours':'text','ID extrait':'text','Pays':'text','Institution':'text','Langue':['fr','en','ar','pt','es','sw','Autre'],'Date discours':'date','Ordre affirmation':'text','Corpus':['Trois premières affirmations','Complément pédagogique','Hors période justifié','Décision chiffrée distincte'],
  'Motif de sélection':'long','Extrait original':'long','Traduction FR':'long','Statut du chiffre':['Résultat observé','Estimation','Prévision / projection','Objectif','Décision annoncée','À préciser'],
  'ID source de vérification':'text','Version disponible à la date du discours':'text','Version ultérieure distincte':'text',
  **{c:['Satisfait','Partiel','Difficulté constatée','Non applicable','Non vérifiable'] for c in ['Source','Période','Définition et champ','Unité et calcul','Comparabilité','Interprétation','Clarté','Limites et incertitude']},
  **{c+' — justification':'long' for c in ['Source','Période','Définition et champ','Unité et calcul','Comparabilité','Interprétation','Clarté','Limites et incertitude']},
  'Conclusion limitée à la preuve':'long','Destination':['L2','Complément documentaire L3']}},
 'constats':{'title':'Constats et recommandations','columns':{
  'ID constat':'text','Thème':['Demande','Priorisation','Sources','Vérification','Communication','Correction','Suivi','Coordination','Langues'],
  'Problème':'long','Statut preuve':['Déclaré','Documenté','Concordant','Contradictoire','Non établi'],
  'IDs réponses / documents / extraits':'long','Contrainte':'long','Action':'long','Responsable':'text','Moyens existants':'long','Échéance indicative':'text','Preuve de fonctionnement':'long',
  'Outil':['O1','O2','O3','O4','O5','O6','O7','O8','Transversal'],'Destination':['L2 documentaire','L3'],'Limites':'long'}},
 'traductions':{'title':'Traductions et décisions de codage','columns':{
  'ID réponse ou instrument':'text','Code champ':'text','Langue originale':['fr','en','ar','pt','es','sw','Autre'],'Texte original':'long','Traduction FR':'long',
  'Moyen utilisé':'text','Relecteur':'text','Date relecture':'date','Statut':['À traduire','Traduite non relue','Relue','Ambiguïté à clarifier'],
  'Divergence et arbitrage':'long','Code analytique retenu':'text','État retenu':['RENSEIGNE','NR','NSP','NA','NF','FILTRE_INDETERMINE','À contrôler'],
  'Autorisation attribution vérifiée':['Non','Oui','Non applicable'],'Version':'text'}},
 'tests':{'title':'Tests réels des outils O1–O8','columns':{
  'ID testeur pseudonymisé':'text','Institution / profil':'text','Profil':['INS','Autre producteur','Utilisateur','Partenaire','Contrôle interne'],
  'Version prototype':'text','Date test':'date','Langue essai':['fr','en','ar','pt','es','Autre'],'Langue support':'text',
  'Nature':['Essai utilisateur réel','Contrôle interne','Simulation technique'],'Tâche':['T1 Source','T2 Sens','T3 Comparaison','T4 Passage','T5 Vérification'],
  'Support public ou exercice identifié':'long','Consigne':'long','Réponse produite':'long','Temps déclaré ou mesuré':'text','Aide reçue':'long',
  'Résultat':['Réussite autonome','Réussite avec aide','Échec','Non observée'],'Difficulté':'long','Gravité':['Bloquante','Importante','Mineure','Aucune'],
  'Correction décidée':'long','Version corrigée':'text','ID contre-test':'text','Destination':['L3 préparation','L4']}},
 'observations':{'title':'Revue des livrables','columns':{
  'ID observation':'text','Auteur / profil autorisé':'text','Livrable':['L1','L2','L3','L4','L5'],'Version examinée':'text','Date réception':'date','Langue':'text','Observation':'long',
  'Décision':['À examiner','Retenue','Retenue en partie','Non retenue avec justification','À arbitrer'],'Justification':'long','Modification':'long','Localisation':'text','Responsable':'text',
  'Statut':['Ouverte','En cours','Traitée','Clôturée'],'Preuve de clôture':'long'}},
 'recette':{'title':'Contrôle des 25 combinaisons','columns':{
  'Parcours':['ins','producteurs','utilisateurs','partenaires','abrege'],'Langue':['fr','en','ar','pt','es'],'Version':'text','Date':'date','Vérificateur':'text',
  **{k:['À vérifier','Conforme','Anomalie','Non applicable'] for k in ['Sens et traduction','Notices et autorisations','Codes et filtres','Choix exclusifs','Changement de langue','Brouillon','Envoi partiel','Enregistrement distant','Accusé','Export UTF-8','Téléphone','Arabe RTL']},
  'Anomalie et correction':'long','Statut final':['À vérifier','Prête techniquement','Relecture linguistique effectuée','Autorisation de diffusion donnée'],'Preuve':'long'}}
}

def latest_registry(events,kind=None,include_tests=False):
    latest={}
    for e in events:
        if kind and e['kind']!=kind:continue
        key=e['record_id']
        if key not in latest or e['version']>latest[key]['version']:latest[key]=e
    return sorted([e for e in latest.values() if include_tests or not e.get('is_test',True)],key=lambda e:(e.get('created_at',''),e['record_id']))

def validate_registry(kind,data):
    if kind not in REGISTERS or not isinstance(data,dict):raise ValidationError('Registre inconnu.')
    allowed=REGISTERS[kind]['columns']
    if set(data)-set(allowed):raise ValidationError('Colonnes inconnues : '+', '.join(sorted(set(data)-set(allowed))))
    if not any(str(v).strip() for v in data.values()):raise ValidationError('Enregistrement vide.')
    for k,v in data.items():
        if not isinstance(v,str) or len(v)>12000 or '\x00' in v:raise ValidationError(k+' : texte invalide.')
        rule=allowed[k]
        if v and isinstance(rule,list) and v not in rule:raise ValidationError(k+' : modalité inconnue.')
        if v and rule=='date':
            try:datetime.strptime(v,'%Y-%m-%d')
            except ValueError as e:raise ValidationError(k+' : utiliser AAAA-MM-JJ.') from e
    if kind=='invitations' and data.get('Statut') in ('Envoyé','Relancé','Réponse reçue') and not data.get('Date invitation'):raise ValidationError('Date réelle d’invitation requise pour ce statut.')
    if kind=='discours':
        for c in ['Source','Période','Définition et champ','Unité et calcul','Comparabilité','Interprétation','Clarté','Limites et incertitude']:
            if data.get(c) and not data.get(c+' — justification'):raise ValidationError(c+' : renseigner la justification.')
    if kind=='constats' and data.get('Destination')=='L2 documentaire' and 'REP-' in data.get('IDs réponses / documents / extraits',''):
        raise ValidationError('Un constat L2 ne peut pas utiliser une réponse de consultation. Choisir L3 ou retirer cette source.')
    if kind=='recette' and data.get('Statut final')=='Autorisation de diffusion donnée':
        checks=[k for k in allowed if isinstance(allowed[k],list) and 'Conforme' in allowed[k]]
        if any(data.get(k) not in ('Conforme','Non applicable') for k in checks) or not data.get('Preuve') or not data.get('Vérificateur') or not data.get('Date'):
            raise ValidationError('Clore tous les contrôles et renseigner la preuve, le vérificateur et la date avant autorisation.')
    if kind=='observations' and data.get('Statut')=='Clôturée' and not data.get('Preuve de clôture'):raise ValidationError('Une preuve de clôture est nécessaire.')
    return data

def register_csv_templates():
    b=io.BytesIO()
    with zipfile.ZipFile(b,'w',zipfile.ZIP_DEFLATED) as z:
        for kind,meta in REGISTERS.items():z.writestr(kind+'.csv',csv_bytes([],list(meta['columns'])))
    return b.getvalue()

def descriptive_summary(records):
    # Only reviewed usable real responses; never count records as independent countries.
    included=[r for r in records if not r['payload'].get('is_test',True) and (r.get('review') or {}).get('status')=='Exploitable']
    result=[]
    for form_id,form in FORMS.items():
        group=[r for r in included if r['payload']['form_id']==form_id]
        inst={(str(r['payload']['answers'].get(form['country'],'')).strip().casefold(),str(r['payload']['answers'].get(form['institution'],'')).strip().casefold()) for r in group if r['payload']['answers'].get(form['institution'])}
        result.append({'Profil':form_id,'Contributions réelles exploitables':len(group),'Institutions déclarées distinctes (à harmoniser)':len(inst),'Sans institution déclarée':sum(not r['payload']['answers'].get(form['institution']) for r in group)})
    return result

def coverage_rows(records):
    """Descriptive coverage; no response rate or implicit institutional consolidation."""
    groups={}
    for r in records:
        p=r['payload']
        if p.get('is_test',True):continue
        form=FORMS.get(p.get('form_id'))
        if not form:continue
        a=p.get('answers',{});origin=a.get('RA01_Profil','') if p['form_id']=='abrege' else ''
        key=(str(a.get(form['country']) or 'Non renseigné').strip(),p['form_id'],str(origin),str(a.get(form.get('sector','')) or 'Non renseigné').strip(),p.get('language',''),p.get('schema_version',''))
        g=groups.setdefault(key,{'received':0,'usable':0,'institutions':set(),'missing':0,'individual':0})
        g['received']+=1;g['usable']+=(r.get('review') or {}).get('status')=='Exploitable'
        institution=str(a.get(form['institution']) or '').strip()
        if institution:g['institutions'].add(institution.casefold())
        else:g['missing']+=1
    return [dict(zip(['Pays / zone déclaré','Parcours','Profil RA déclaré','Domaine déclaré','Langue interface','Version'],k),**{'Contributions réelles reçues':v['received'],'Contributions classées exploitables':v['usable'],'Institutions déclarées distinctes (à harmoniser)':len(v['institutions']),'Sans institution déclarée':v['missing']}) for k,v in sorted(groups.items())]

# Public collection UI, interface patch 2.1.0. Instrument codes remain unchanged.
import streamlit as st
import time
import logging
from pathlib import Path
from collections.abc import Mapping

MAIN_TITLE = 'Statistical support for decision-makers’ speeches'
SAFE_REJECTIONS = {'INVALID_ACCESS_CODE', 'COLLECTION_CLOSED', 'INVALID_WRITER',
                   'INVALID_SUBMISSION', 'QUOTA_REACHED', 'TEST_MODE_MISMATCH'}


def read_config():
    try:
        return dict(st.secrets)
    except FileNotFoundError:
        return {}


def get_answers(form_id):
    return st.session_state.setdefault('answers_' + form_id, {})


def clear_widget_keys(form_id=None):
    prefixes = ('w|', 'add|', 'remove|') if form_id is None else tuple(
        p + form_id + '|' for p in ('w|', 'add|', 'remove|'))
    for key in list(st.session_state):
        if key.startswith(prefixes):
            del st.session_state[key]
        elif (key.startswith(('authority_', 'confirm_')) and
              (form_id is None or key.endswith('_' + form_id))):
            del st.session_state[key]


def on_language_change():
    # Canonical answers are independent of the rendered widget language.
    clear_widget_keys()


def _store_answers(form_id, answers):
    if answers.get(FORMS[form_id]['consent']) != 1:
        answers = {FORMS[form_id]['consent']: answers.get(FORMS[form_id]['consent'])}
    st.session_state['answers_' + form_id] = field_states(form_id, answers)[0]
    clear_widget_keys(form_id)
    st.session_state.pop('submit_messages_' + form_id, None)


def capture_answer(form_id, fid, key):
    answers = dict(get_answers(form_id))
    answers[fid] = st.session_state[key]
    prefix = FORMS[form_id].get('priority_prefix')
    if prefix and fid in [prefix + '_P' + str(i) for i in (1, 2, 3)]:
        # Earlier ranks take precedence. Clear downstream ranks made invalid by editing.
        seen = set()
        gap = False
        for rank in (1, 2, 3):
            target = prefix + '_P' + str(rank)
            value = answers.get(target)
            if gap or (rank > 1 and answers.get(prefix + '_P1') in (0, 98)) or value in seen:
                answers.pop(target, None)
                gap = True
            elif blank(value):
                gap = True
            else:
                seen.add(value)
    _store_answers(form_id, answers)


def add_answer(form_id, fid, key):
    value = st.session_state.get(key)
    field = next(f for _, f in iter_fields(form_id) if f['id'] == fid)
    answers = dict(get_answers(form_id))
    selected = list(answers.get(fid) or [])
    if value in remaining_choices(field, selected):
        answers[fid] = selected + [value]
    _store_answers(form_id, answers)


def remove_answer(form_id, fid, value):
    answers = dict(get_answers(form_id))
    answers[fid] = [v for v in (answers.get(fid) or []) if v != value]
    _store_answers(form_id, answers)


def render_field(form_id, field, lang):
    fid = field['id']
    key = f'w|{form_id}|{lang}|{fid}'
    answers = get_answers(form_id)
    label = text(field['label'], lang)
    options = {o['code']: text(o['label'], lang) for o in field['options']}
    if field['type'] == 'multi':
        selected = list(answers.get(fid) or [])
        st.markdown('**' + label + '**')
        for value in selected:
            col_text, col_remove = st.columns([5, 1])
            col_text.text(options.get(value, str(value)))
            col_remove.button(ui('remove_choice', lang),
                              key=f'remove|{form_id}|{lang}|{fid}|{value}',
                              on_click=remove_answer, args=(form_id, fid, value))
        remaining = remaining_choices(field, selected)
        if remaining:
            add_key = f'add|{form_id}|{lang}|{fid}'
            if add_key not in st.session_state:
                st.session_state[add_key] = None
            st.selectbox(ui('add_choice', lang) + ' — ' + label, [None, *remaining],
                         key=add_key,
                         format_func=lambda v: ui('choose_next', lang) if v is None else options[v],
                         on_change=add_answer, args=(form_id, fid, add_key))
        else:
            st.caption(ui('multi_complete', lang))
        st.caption(ui('multi_help', lang))
        if field.get('max_choices'):
            st.caption(ui('max_choices', lang, n=field['max_choices']))
        return
    if key not in st.session_state:
        value = answers.get(fid)
        st.session_state[key] = ('' if value is None else value) if field['type'] in (
            'text', 'textarea', 'month', 'email') else value
    kwargs = {'key': key, 'on_change': capture_answer, 'args': (form_id, fid, key)}
    if field['type'] == 'single':
        allowed = priority_choices(form_id, field, answers)
        if st.session_state.get(key) not in [None, *allowed]:
            st.session_state[key] = None
            answers.pop(fid, None)
        st.selectbox(label, [None, *allowed],
                     format_func=lambda v: ui('empty', lang) if v is None else options[v],
                     disabled=not allowed, **kwargs)
        prefix = FORMS[form_id].get('priority_prefix')
        if prefix and fid in (prefix + '_P2', prefix + '_P3'):
            st.caption(ui('priority_help', lang))
    elif field['type'] == 'textarea':
        st.text_area(label, height=115, max_chars=field.get('max_chars', 2500), **kwargs)
        if field.get('max_words'):
            st.caption(ui('words', lang, n=field['max_words']))
    else:
        st.text_input(label, max_chars=field.get('max_chars', 500), **kwargs)
    get_answers(form_id)[fid] = st.session_state[key]


def safe_public_error(code, lang):
    messages = {'COLLECTION_CLOSED': 'closed', 'INVALID_ACCESS_CODE': 'invalid_access',
                'SUBMISSION_CONFLICT': 'conflict', 'NETWORK_UNCERTAIN': 'pending',
                'TEST_MODE_MISMATCH': 'mode_mismatch'}
    return ui(messages.get(code, 'service_error'), lang)


def perform_submission(store, pending, access, lang):
    try:
        receipt = store.submit(pending['payload'], pending['contacts'], access)
    except StorageError as exc:
        st.session_state['last_submit_error'] = str(exc)
    else:
        st.session_state['last_success'] = {'payload': pending['payload'], 'receipt': receipt}
        st.session_state.pop('pending', None)
        st.session_state.pop('last_submit_error', None)
    st.rerun()


def campaign_status(store, settings, force=False):
    if store is None:
        return None, ''
    cache_key = (settings.mode, settings.url, settings.campaign, settings.test_mode,
                 hashlib.sha256(settings.writer_token.encode()).hexdigest())
    cached = st.session_state.get('campaign_cache_v21')
    if not force and cached and cached[0] == cache_key and time.monotonic() - cached[1] < 15:
        return cached[2], ''
    try:
        info = store.campaign_info()
    except StorageError as exc:
        st.session_state.pop('campaign_cache_v21', None)
        return None, str(exc)
    st.session_state['campaign_cache_v21'] = (cache_key, time.monotonic(), info)
    return info, ''


def show_header(form_id, lang):
    # First visible element in the main page, including pending/receipt/error screens.
    st.title(MAIN_TITLE)
    if lang != 'fr':
        st.caption(ui('title', lang))
    st.header(text(FORMS[form_id]['title'], lang))
    st.caption('HISWACA – SOP 1 · P178497 · ET-AUC-478525-CS-INDV · v' + APP_VERSION)
    st.caption(ui('dates', lang))
    if lang == 'ar':
        st.markdown('<style>[data-testid="stMarkdownContainer"], [data-testid="stWidgetLabel"], '
                    'textarea, input {direction:rtl;text-align:right;unicode-bidi:plaintext;} '
                    'h1 {direction:ltr!important;text-align:left!important;} '
                    'pre,code {direction:ltr;text-align:left;} [data-baseweb="select"] '
                    '{direction:rtl;}</style>', unsafe_allow_html=True)


def initialise_navigation():
    """Separate URL routing, widget selection and canonical answers."""
    if 'draft_to_apply' in st.session_state:
        data = st.session_state.pop('draft_to_apply')
        clear_widget_keys()
        for key in ('ui_form', 'ui_language', 'upload_draft'):
            st.session_state.pop(key, None)
        st.session_state['ui_form'] = data['form_id']
        st.session_state['ui_language'] = data['language']
        st.session_state['answers_' + data['form_id']] = data['answers']
        st.session_state['id_' + data['form_id']] = data['submission_id']
        st.session_state['meta_' + data['form_id']] = data.get('metadata', {})
        st.session_state['step_' + data['form_id']] = FORMS[data['form_id']]['sections'][0]['id']
        st.query_params['form'] = data['form_id']
        st.query_params['lang'] = data['language']
        st.session_state['_last_url'] = (data['form_id'], data['language'])
    qform = st.query_params.get('form', 'ins')
    qlang = st.query_params.get('lang', 'fr')
    target = (qform if qform in FORMS else 'ins', qlang if qlang in LANGUAGES else 'fr')
    last_url = st.session_state.get('_last_url')
    if last_url is not None and target != last_url:
        if target[1] != st.session_state.get('ui_language'):
            clear_widget_keys()
        st.session_state['ui_form'], st.session_state['ui_language'] = target
    st.session_state.setdefault('ui_form', target[0])
    st.session_state.setdefault('ui_language', target[1])
    if st.session_state['ui_form'] not in FORMS:
        st.session_state['ui_form'] = target[0]
    if st.session_state['ui_language'] not in LANGUAGES:
        st.session_state['ui_language'] = target[1]


def jump_to(step_key, section_id):
    st.session_state[step_key] = section_id
    st.rerun()


def main():
    st.set_page_config(page_title=MAIN_TITLE, page_icon='📋', layout='centered')
    initialise_navigation()
    st.sidebar.selectbox('Langue / Language / اللغة', LANGUAGES, key='ui_language',
                         format_func=lambda lang: LANG_NAMES[lang], on_change=on_language_change)
    lang = st.session_state['ui_language']
    locked = 'pending' in st.session_state or 'last_success' in st.session_state
    st.sidebar.selectbox(ui('form', lang), list(FORMS), key='ui_form', disabled=locked,
                         format_func=lambda f: text(FORMS[f]['title'], lang))
    form_id = st.session_state['ui_form']
    if 'pending' in st.session_state:
        form_id = st.session_state['pending']['payload']['form_id']
    elif 'last_success' in st.session_state:
        form_id = st.session_state['last_success']['payload']['form_id']
    st.query_params['form'] = form_id
    st.query_params['lang'] = lang
    st.session_state['_last_url'] = (form_id, lang)
    show_header(form_id, lang)
    cfg = read_config()
    try:
        settings = Settings.from_mapping(cfg)
    except ConfigurationError:
        # Do not display raw secrets or backend responses to participants.
        logging.getLogger('hiswaca').error('HISWACA_CONFIGURATION_INVALID : vérifier les clés de configuration et la notice.')
        st.error(ui('service_error', lang))
        st.caption(ui('error_code', lang, code='HISWACA_CONFIGURATION_INVALID'))
        st.stop()
    store = make_store(settings)
    form = FORMS[form_id]
    if settings.contact_email:
        st.caption(ui('contact', lang) + ' : ' + settings.contact_email)
    if settings.mode == 'preview':
        st.warning(ui('preview', lang))
    elif settings.test_mode:
        st.warning(ui('test', lang))
    if settings.mode == 'production' and not settings.test_mode and lang not in settings.approved_languages:
        st.warning(ui('unreviewed', lang))
    with st.sidebar.expander(ui('quick_help', lang)):
        st.markdown(ui('quick_help_text', lang))
    guide_path = Path(__file__).resolve().parent / 'docs' / 'Guide_remplissage_HISWACA_FR.docx'
    if guide_path.is_file():
        st.sidebar.download_button(ui('guide_fr', lang), guide_path.read_bytes(),
                                   file_name=guide_path.name,
                                   mime='application/vnd.openxmlformats-officedocument.wordprocessingml.document',
                                   key='guide_download')
    if 'pending' in st.session_state:
        pending = st.session_state['pending']
        error = st.session_state.get('last_submit_error', 'NETWORK_UNCERTAIN')
        st.warning(safe_public_error(error, lang))
        st.code(pending['payload']['submission_id'], language=None)
        access = st.text_input(ui('access', lang), type='password', key='retry_code')
        st.download_button(ui('pending_download', lang),
                           json.dumps({'kind': 'hiswaca_pending', **pending}, ensure_ascii=False, indent=2).encode(),
                           file_name='HISWACA_pending.json', mime='application/json')
        st.caption(ui('draft_warning', lang))
        if st.button(ui('retry', lang), type='primary', disabled=store is None):
            # A fresh campaign check must NOT block an idempotent retry after closing.
            perform_submission(store, pending, access, lang)
        if error in SAFE_REJECTIONS and st.button(ui('rejected_back', lang), key='back_rejected'):
            st.session_state.pop('pending', None)
            st.session_state.pop('last_submit_error', None)
            jump_to('step_' + form_id, 'review')
        st.stop()
    if 'last_success' in st.session_state:
        result = st.session_state['last_success']
        payload, receipt = result['payload'], result['receipt']
        st.success(ui('success_test' if payload['is_test'] else 'success', lang))
        st.code(receipt['receipt'], language=None)
        st.caption('UTC : ' + receipt['received_at'])
        st.download_button(ui('receipt', lang), receipt_text(payload, receipt).encode(),
                           file_name=receipt['receipt'] + '.txt', mime='text/plain')
        st.info(ui('supplement', lang))
        if form_id == 'ins':
            st.info(text(FORMS['ins']['relay_note'], lang))
        if st.button(ui('new', lang)):
            for key in list(st.session_state):
                del st.session_state[key]
            st.session_state['ui_language'] = lang
            st.rerun()
        st.stop()
    st.caption(text(form['duration'], lang) + ' · ' + form['version'])
    meta = st.session_state.setdefault('meta_' + form_id, {
        k: str(st.query_params.get(q, ''))[:120] for k, q in [('relay_id', 'relay'), ('invitation_id', 'inv')]
        if st.query_params.get(q)})
    if form_id == 'ins':
        st.info(text(form['relay_note'], lang))
    if form_id == 'abrege':
        st.info(ui('short', lang))
    with st.expander(ui('notice', lang), expanded=not get_answers(form_id)):
        st.markdown(text(NOTICE, lang))
        custom = cfg.get('notice', {})
        if isinstance(custom, Mapping) and custom.get(lang):
            st.text(str(custom[lang]))
    st.sidebar.download_button(ui('fallback', lang), fallback_text(form_id, lang),
                               file_name=f'HISWACA_{form_id}_{lang}.txt', mime='text/plain')
    with st.sidebar.expander(ui('resume', lang)):
        upload = st.file_uploader(ui('upload', lang), type=['json'], key='upload_draft')
        if st.button(ui('import', lang), disabled=upload is None):
            try:
                raw = upload.getvalue()
                kind = json.loads(raw.decode('utf-8-sig')).get('kind') if len(raw) <= MAX_DRAFT_BYTES else None
                if kind == 'hiswaca_pending':
                    st.session_state['pending'] = load_pending(raw, settings.test_mode)
                else:
                    st.session_state['draft_to_apply'] = load_draft(raw)
            except (ValueError, AttributeError, ValidationError):
                st.error(ui('invalid_file', lang))
            else:
                st.rerun()
    answers = get_answers(form_id)
    uid = st.session_state.setdefault('id_' + form_id, str(uuid4()))
    sections = active_sections(form_id, answers)
    ids = [s['id'] for s in sections]
    step_key = 'step_' + form_id
    current = st.session_state.setdefault(step_key, ids[0])
    if current not in ids + ['review']:
        current = ids[0]
        st.session_state[step_key] = current
    if current != 'review':
        section = next(s for s in sections if s['id'] == current)
        index = ids.index(current)
        st.progress(index / (len(sections) + 1), text=ui('progress', lang, n=index + 1, total=len(sections)))
        st.subheader(section['id'] + ' — ' + text(section['title'], lang))
        if text(section['note'], lang):
            st.markdown(text(section['note'], lang))
        if evaluate(section.get('gate'), answers) is None:
            st.info(ui('indeterminate', lang))
        else:
            for field in section['fields']:
                _, states = field_states(form_id, get_answers(form_id))
                if states[field['id']] not in ('NF', 'FILTRE_INDETERMINE'):
                    render_field(form_id, field, lang)
        answers = get_answers(form_id)
        consent = answers.get(form['consent'])
        eligible = not (form_id == 'producteurs' and answers.get('QP01_Produit') == 0)
        if consent == 0:
            st.info(ui('refused', lang))
        elif not eligible:
            st.info(ui('ineligible', lang))
        elif consent == 1:
            visible = [s['id'] for s in active_sections(form_id, answers)]
            index = visible.index(current) if current in visible else 0
            c1, c2, c3 = st.columns(3)
            if c1.button(ui('previous', lang), disabled=index == 0, key='prev'):
                jump_to(step_key, visible[max(0, index - 1)])
            if c2.button(ui('next', lang), type='primary', key='next'):
                jump_to(step_key, visible[index + 1] if index + 1 < len(visible) else 'review')
            if c3.button(ui('review', lang), key='review'):
                jump_to(step_key, 'review')
    else:
        st.progress(1.0, text=ui('review_title', lang))
        if st.button(ui('previous_review', lang), key='prev_review_top'):
            jump_to(step_key, ids[-1])
        st.info(ui('partial', lang))
        errors, warnings = validation(form_id, answers, lang)
        effective, states = field_states(form_id, answers)
        for section in sections:
            with st.expander(section['id'] + ' — ' + text(section['title'], lang)):
                for field in section['fields']:
                    if not blank(effective.get(field['id'])):
                        st.text(text(field['label'], lang) + '\n' + answer_label(field, effective[field['id']], lang))
                if st.button(ui('edit', lang), key='edit_' + section['id']):
                    jump_to(step_key, section['id'])
        for warning in warnings:
            st.warning(warning)
        attr = effective.get(form['attribution'])
        authority = True
        if attr in (1, 2):
            authority = st.checkbox(ui('authority', lang), key='authority_' + form_id)
        else:
            st.caption(ui('anonymous', lang))
        refresh = st.button(ui('refresh', lang), key='refresh_campaign')
        info, storage_error = campaign_status(store, settings, force=refresh)
        if info and info.get('opens_at') and info.get('closes_at'):
            st.caption(ui('campaign_dates', lang, start=info['opens_at'], end=info['closes_at']))
        if storage_error:
            st.caption(ui('error_code', lang, code=storage_error))
        access = st.text_input(ui('access', lang), type='password', key='access') if info and info.get('requires_access_code') else ''
        confirmed = st.checkbox(ui('confirm', lang), key='confirm_' + form_id)
        reasons = submission_requirements(form_id, answers, lang, settings, info,
                                         confirmed, authority, access, storage_error)
        st.subheader(ui('checklist', lang))
        if reasons:
            for reason in reasons:
                st.info(reason)
        else:
            st.success(ui('ready', lang))
        c_previous, c_submit = st.columns([1, 2])
        if c_previous.button(ui('previous_review', lang), key='prev_review'):
            jump_to(step_key, ids[-1])
        # Always clickable: submission is validated on click, never silently disabled.
        if c_submit.button(ui('submit', lang), type='primary', key='submit'):
            fresh_info, fresh_error = campaign_status(store, settings, force=True)
            blocks = submission_requirements(form_id, answers, lang, settings, fresh_info,
                                              confirmed, authority, access, fresh_error)
            if blocks:
                st.error(ui('not_sent', lang))
                for block in blocks:
                    st.error(block)
            else:
                try:
                    payload, contacts = prepare_submission(form_id, answers, lang, uid, settings.test_mode, meta)
                except ValidationError as exc:
                    st.error(str(exc))
                else:
                    payload['attribution_authority_confirmed'] = bool(attr in (1, 2) and authority)
                    st.session_state['pending'] = {'payload': payload, 'contacts': contacts}
                    perform_submission(store, st.session_state['pending'], access, lang)
    if get_answers(form_id).get(form['consent']) == 1:
        with st.sidebar.expander(ui('draft', lang)):
            st.caption(ui('draft_warning', lang))
            st.download_button(ui('download_draft', lang), draft_bytes(form_id, get_answers(form_id), lang, uid, meta),
                               file_name=f'HISWACA_brouillon_{form_id}.json', mime='application/json',
                               key='download_' + form_id)
    st.caption(ui('not_validation', lang))


if __name__ == '__main__':
    main()
