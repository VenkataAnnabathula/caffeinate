# ☕ caffeinate

AI-powered **RAG Analytics Dashboard** (professional MVP)

## Structure
- \ackend/\ → FastAPI APIs (ingestion, RAG, query)
- \rontend/\ → Streamlit dashboard + chat
- \postgres/\ → DB container + schema seed
- \infra/\ → docker-compose (local), k8s manifests, terraform (AWS)

## Tech Stack
Frontend: Streamlit  
Backend: FastAPI (Python)  
DB: PostgreSQL (RDS later) + S3 (uploads)  
Vector DB: Pinecone  
LLM: Gemini Pro  
Infra: Docker, Kubernetes, Terraform

## Goal
Enable SMBs to upload data, see dashboards, and ask NL questions answered via RAG over structured data → SQL → results.

