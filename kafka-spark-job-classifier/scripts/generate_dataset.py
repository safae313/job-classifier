"""Génère un jeu d'entraînement étiqueté (data/train.csv) d'offres d'emploi FR/EN.

Les offres sont synthétiques (gabarits + vocabulaire métier). Un peu de « bruit »
est ajouté (compétences d'un autre domaine) pour éviter un problème trop facile.
Pour de meilleurs résultats sur des données réelles, ajouter des offres réelles
étiquetées à la main dans le même CSV (colonnes text,label).

Usage : python scripts/generate_dataset.py [--per-class 1000]
"""
import argparse
import csv
import random
from pathlib import Path

DOMAINS = {
    "Data": {
        "titles": ["Data Scientist", "Data Analyst", "Data Engineer", "Ingénieur Big Data",
                   "Ingénieur Machine Learning", "Analyste BI", "Consultant Business Intelligence",
                   "Ingénieur MLOps", "Statisticien", "Développeur BI", "Machine Learning Engineer",
                   "Analytics Engineer", "Chargé d'études statistiques", "Ingénieur IA"],
        "missions_fr": ["Concevoir et maintenir des pipelines ETL",
                        "Construire des modèles de machine learning pour prédire le churn",
                        "Analyser de grands volumes de données clients",
                        "Créer des tableaux de bord pour la direction",
                        "Mettre en place un data lake et un data warehouse",
                        "Industrialiser des modèles de deep learning",
                        "Réaliser des analyses statistiques et des tests A/B",
                        "Développer des modèles NLP sur des textes clients",
                        "Garantir la qualité et la gouvernance des données",
                        "Exploiter les données pour la recommandation de produits"],
        "missions_en": ["build and maintain ETL pipelines",
                        "train machine learning models to forecast demand",
                        "analyze large customer datasets",
                        "design dashboards and reports for business teams",
                        "build a modern data platform",
                        "deploy deep learning models in production",
                        "run statistical analyses and A/B tests",
                        "develop NLP models on customer feedback"],
        "skills": ["Python", "SQL", "Spark", "Hadoop", "Pandas", "Scikit-learn", "TensorFlow",
                   "PyTorch", "Power BI", "Tableau", "Airflow", "Kafka", "Databricks",
                   "statistiques", "dbt", "Snowflake", "R", "NumPy", "MLflow", "Hive"],
    },
    "Développement": {
        "titles": ["Développeur Java/Spring Boot", "Développeur Full Stack", "Développeur Front-End React",
                   "Développeur Back-End Node.js", "Développeur Mobile Flutter", "Ingénieur Logiciel",
                   "Développeur PHP Symfony", "Développeur .NET", "Software Engineer",
                   "Développeur Angular", "Développeuse Web", "Backend Developer",
                   "Mobile Developer", "Ingénieur Études et Développement"],
        "missions_fr": ["Développer de nouvelles fonctionnalités sur notre application web",
                        "Concevoir et développer des API REST",
                        "Participer aux revues de code et à l'architecture applicative",
                        "Maintenir et faire évoluer une application mobile",
                        "Écrire des tests unitaires et d'intégration",
                        "Intégrer des maquettes Figma",
                        "Travailler en méthode Agile Scrum avec l'équipe produit",
                        "Refondre une application legacy en microservices",
                        "Corriger les anomalies et optimiser les performances"],
        "missions_en": ["develop new features for our web platform",
                        "design and build REST APIs",
                        "take part in code reviews",
                        "maintain our iOS and Android apps",
                        "write unit and integration tests",
                        "turn Figma designs into responsive interfaces",
                        "work in an agile Scrum team"],
        "skills": ["Java", "Spring Boot", "React", "Angular", "Vue.js", "TypeScript", "JavaScript",
                   "Node.js", "PHP", "Symfony", "Laravel", "C#", ".NET", "Flutter", "Kotlin",
                   "Swift", "Git", "JUnit", "Hibernate", "HTML/CSS", "Next.js", "Django"],
    },
    "Sécurité": {
        "titles": ["Analyste SOC", "Ingénieur Cybersécurité", "Pentester", "Consultant GRC",
                   "RSSI", "Ingénieur Sécurité Réseau", "Security Engineer",
                   "Analyste Threat Intelligence", "Auditeur Sécurité des SI", "Expert IAM",
                   "Security Analyst", "Consultant Cybersécurité", "Incident Responder"],
        "missions_fr": ["Surveiller les alertes de sécurité et répondre aux incidents",
                        "Réaliser des tests d'intrusion sur les applications web",
                        "Accompagner la mise en conformité ISO 27001",
                        "Piloter la gestion des vulnérabilités et le patch management",
                        "Administrer les pare-feux et les VPN",
                        "Mener des analyses forensiques après incident",
                        "Sensibiliser les collaborateurs au phishing",
                        "Définir la politique de gestion des identités et des accès",
                        "Réaliser des audits de sécurité et des analyses de risques"],
        "missions_en": ["monitor security alerts and respond to incidents",
                        "perform penetration tests on web applications",
                        "lead ISO 27001 compliance efforts",
                        "manage vulnerabilities and patching",
                        "investigate breaches with forensic analysis",
                        "run phishing awareness campaigns",
                        "harden identity and access management"],
        "skills": ["SIEM", "Splunk", "QRadar", "Burp Suite", "Metasploit", "Nessus", "Wireshark",
                   "ISO 27001", "EBIOS", "Fortinet", "Palo Alto", "CEH", "OSCP", "CISSP", "EDR",
                   "Active Directory", "IAM", "OWASP", "Kali Linux", "NIST"],
    },
    "Cloud": {
        "titles": ["Ingénieur DevOps", "Architecte Cloud", "Ingénieur Cloud AWS", "Ingénieur SRE",
                   "Administrateur Azure", "Ingénieur Plateforme Kubernetes", "Cloud Engineer",
                   "Consultant Cloud GCP", "Ingénieur Infrastructure as Code", "FinOps Engineer",
                   "DevOps Engineer", "Site Reliability Engineer", "Ingénieur Systèmes Cloud"],
        "missions_fr": ["Automatiser les déploiements via des pipelines CI/CD",
                        "Migrer des applications on-premise vers le cloud",
                        "Administrer des clusters Kubernetes en production",
                        "Provisionner l'infrastructure avec Terraform",
                        "Mettre en place la supervision et l'alerting",
                        "Optimiser les coûts cloud",
                        "Garantir la haute disponibilité des services",
                        "Conteneuriser les applications existantes",
                        "Concevoir une architecture multi-cloud résiliente"],
        "missions_en": ["automate deployments with CI/CD pipelines",
                        "migrate workloads to the cloud",
                        "operate Kubernetes clusters in production",
                        "provision infrastructure with Terraform",
                        "set up monitoring and alerting",
                        "reduce cloud costs",
                        "keep our platform highly available"],
        "skills": ["AWS", "Azure", "GCP", "Kubernetes", "Docker", "Terraform", "Ansible", "Jenkins",
                   "GitLab CI", "Helm", "Prometheus", "Grafana", "Linux", "OpenShift", "ArgoCD",
                   "CloudFormation", "EKS", "Bash"],
    },
}

COMPANIES_FR = ["Une startup fintech", "Un grand groupe télécom", "Une ESN internationale",
                "Une banque marocaine", "Un cabinet de conseil", "Une scale-up e-commerce",
                "Un acteur de l'assurance", "Notre client, leader de l'industrie",
                "Une entreprise du secteur public", "Un éditeur de logiciels SaaS"]
COMPANIES_EN = ["A fintech startup", "A leading telecom operator", "A global consulting firm",
                "A fast-growing SaaS company", "Our client in the banking sector",
                "An e-commerce scale-up", "A healthtech company"]
CITIES = ["Casablanca", "Rabat", "Marrakech", "Tanger", "Agadir", "Fès", "Paris", "Lyon",
          "Montréal", "Bruxelles", "Dubaï", "Remote"]
CONTRACTS_FR = ["CDI", "CDD 12 mois", "Stage PFE 6 mois", "Freelance", "Alternance", "Stage"]
CONTRACTS_EN = ["Full-time", "Contract", "Internship", "Freelance", "Permanent"]
EXP_FR = ["Débutant accepté", "2 à 3 ans d'expérience", "5 ans d'expérience minimum",
          "Profil junior bienvenu", "Bac+5 école d'ingénieur", "Anglais professionnel exigé"]
EXP_EN = ["Junior profiles welcome", "3+ years of experience", "5+ years of experience",
          "Senior level", "Fluent English required"]

TEMPLATES_FR = [
    "{company} recrute un(e) {title} ({contract}) à {city}. {mission}. Compétences : {skills}. {exp}.",
    "#Hiring {title} - {city} | {skills} | {contract}",
    "Nous cherchons un(e) {title} pour {mission_l}. Stack : {skills}.",
    "Offre : {title} H/F. Missions : {mission}. Profil : {exp}, maîtrise de {skills}.",
    "{title} — {contract} — {city}. Vous allez {mission_l}. Environnement technique : {skills}.",
    "Rejoignez-nous en tant que {title} ! {mission}. {exp}. Postulez dès maintenant #emploi",
]
TEMPLATES_EN = [
    "{company} is hiring a {title} in {city}. You will {mission}. Skills: {skills}. {exp}.",
    "We're looking for a {title} ({contract}, {city}). Tech: {skills}. Apply now! #jobs",
    "Job: {title}. Your mission: {mission}. Must have: {skills}. {exp}.",
    "#hiring {title} | {city} | {contract} | {skills}",
]


def make_offer(label: str, rng: random.Random) -> str:
    d = DOMAINS[label]
    skills = rng.sample(d["skills"], k=rng.randint(2, 5))
    if rng.random() < 0.3:  # bruit : une compétence d'un autre domaine
        other = rng.choice([k for k in DOMAINS if k != label])
        skills.append(rng.choice(DOMAINS[other]["skills"]))
        rng.shuffle(skills)
    title = rng.choice(d["titles"])

    if rng.random() < 0.65:
        mission = rng.choice(d["missions_fr"])
        return rng.choice(TEMPLATES_FR).format(
            company=rng.choice(COMPANIES_FR), title=title, contract=rng.choice(CONTRACTS_FR),
            city=rng.choice(CITIES), mission=mission, mission_l=mission[0].lower() + mission[1:],
            skills=", ".join(skills), exp=rng.choice(EXP_FR))
    return rng.choice(TEMPLATES_EN).format(
        company=rng.choice(COMPANIES_EN), title=title, contract=rng.choice(CONTRACTS_EN),
        city=rng.choice(CITIES), mission=rng.choice(d["missions_en"]),
        skills=", ".join(skills), exp=rng.choice(EXP_EN))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--per-class", type=int, default=1000)
    parser.add_argument("--out", default=str(Path(__file__).resolve().parent.parent / "data" / "train.csv"))
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    rng = random.Random(args.seed)
    rows = [(make_offer(label, rng), label) for label in DOMAINS for _ in range(args.per_class)]
    rng.shuffle(rows)

    with open(args.out, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f, quoting=csv.QUOTE_ALL)
        writer.writerow(["text", "label"])
        writer.writerows(rows)
    print(f"{len(rows)} offres écrites dans {args.out}")


if __name__ == "__main__":
    main()
