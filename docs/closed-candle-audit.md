# Audit des bougies utilisées par les calculs et détections

Audit du code SignalMaker au 19 octobre 2026, à partir de `main`
`eac34b60e6d59457cb80d47beb21aff04046d146`, avec la correction de structure 15 min.

## Règle appliquée

Les moteurs reçoivent uniquement les bougies dont l’heure de clôture vérifiable
est atteinte. Les dates numériques des entrées normalisées sont en millisecondes
UTC ; les objets datetime et dates ISO sont également acceptés par le filtre.
Un indicateur explicite `is_closed=false` exclut la bougie, même si sa date est
passée ; `is_closed=true` ne permet jamais de contourner une date future.
Les dates de clôture absentes, invalides ou antérieures à l’ouverture sont exclues.

Le filtre s’applique avant le contrôle de l’historique requis, la recherche de
swings et tous les calculs dérivés. Si l’historique clôturé est insuffisant,
l’analyse est bloquée ou renvoie `insufficient_data` plutôt qu’un signal fondé
sur une bougie encore ouverte.

## Parcours examinés

| Parcours | Constat et protection finale |
| --- | --- |
| Chargement crypto et API de bougies | Absence de filtre SQL. Filtre ajouté avant les limites et les recherches première/dernière bougie. |
| Curseur de collecte crypto | Le maximum pouvait inclure une bougie ouverte. Le curseur ne progresse plus que sur les clôtures vérifiées. |
| Pipeline Wyckoff/SMC | Les séries pouvaient arriver brutes. Filtrage avant continuité, qualité et moteur, sur toutes les unités de temps. |
| Moteur de signaux et moteur legacy | Les appels directs pouvaient contourner le pipeline. Protection de leurs entrées pour RSI, volumes, niveaux jour/semaine/session, liquidité, imbalances, swings, BOS/MSS, hiérarchie et objectifs. |
| Momentum : prix, RSI, EMA, deltas, accélération et structure | Filtre de clôture harmonisé ; structure protégée avant comptage et swings ; bundle filtré également pour les appels directs. |
| Momentum : prix de référence et valorisation fondés sur une bougie | La dernière bougie pouvait être ouverte. SQL filtré, avec repli sur une autre unité clôturée ou sur la référence existante. |
| Actions/ETF : repository, adaptateur et workflow partagé | Le repository excluait déjà les périodes non terminées. L’adaptateur conserve cette règle et rejette aussi les drapeaux texte `false`/`0`. Les moteurs partagés contrôlent l’historique après filtrage. |
| Scheduler actions/ETF | Le maximum brut pouvait déclencher un job sur une bougie ouverte. Sélection limitée aux ouvertures dont la durée d’intervalle est écoulée. |
| Analyse IA : snapshot et outils de lecture | Clôture déjà filtrée ; réutilisation de la protection SQL commune, y compris contre des dates inversées. |
| Import Kraken | Le filtre de clôture existait déjà avant écriture. Le curseur de reprise bénéficie aussi de la lecture SQL protégée. |
| Frontend | Affiche les résultats persistés ; aucun moteur de détection de bougies côté navigateur identifié. |

La composante volume proposée dans la PR #442 appelle le même filtre Momentum.
Son intégration avec ces changements a également été testée.

## Vérifications exécutées

- Suite Python complète : **499 tests réussis, 5 échecs préexistants**.
- Les cinq mêmes échecs ont été reproduits séparément sur une copie antérieure
  aux modifications de cet audit. Ils concernent les listes de workers et les
  scripts de déploiement, dans `test_admin_worker_logs.py` et
  `test_reserved_vm_worker_pid_files.py`.
- Les tests transversaux de politique de clôture, pipeline et scheduler :
  **39 tests réussis**.
- Combinaison avec la PR volume #442 : **297 tests ciblés réussis**, couvrant
  Momentum, structure, volume, Wyckoff partagé, chargements, normalisation,
  pipeline, scheduler et politique de clôture.
- Le faux service du test pipeline existant a été complété pour exécuter le vrai
  contrôle de continuité, au lieu de laisser passer un objet Mock sans longueur.
- `git diff --check` réussi.

Commandes principales :

```sh
.venv/bin/python -m pytest tests -q
.venv/bin/python -m pytest tests/test_closed_candle_policy.py tests/test_pipeline_service.py tests/test_scheduler_service.py -q
```

Les tests HTTP locaux nécessitent un bac autorisant le réseau ; avec le réseau
entièrement isolé, le client de test Starlette/AnyIO restait bloqué. Aucun ordre
réel ni appel à un compte de trading n’a été effectué pour cet audit.

## Portée et mise en service

Cet audit porte sur les chemins de calcul présents dans ce dépôt et sur des
tests locaux. Il ne vérifie pas le logiciel externe Raspberry Executor ni les
données effectivement présentes sur le serveur de production. Les horodatages
du fournisseur et l’horloge du serveur restent la référence pour la clôture.
Les bougies actions/ETF sans heure de fin explicite utilisent la durée nominale
de leur intervalle ; le calcul ne reconstruit pas un calendrier de clôture de
séance à partir des prix.

Les diagnostics de stockage et d’ingestion peuvent toujours compter des données
brutes ouvertes. Les tickers d’exécution, soldes et PnL de compte en temps réel
ne sont pas des calculs d’indicateurs sur bougies et conservent leurs prix live.

Aucune migration n’est nécessaire pour cette politique. Après déploiement,
redémarrer l’API et les workers, puis relancer les analyses pour rafraîchir les
snapshots persistés. Les signaux déjà enregistrés ne sont pas corrigés
rétroactivement par une simple lecture de l’API. Aucun déploiement effectué.
