# CRMTracker & AutoReg v6 User Guide

Welcome to the CRMTracker & AutoReg terminal. This system is designed for high-performance data ingestion, automated tracking, and business intelligence for the telemarketing and financial operations teams.

## Roles and Access
The platform is governed by Role-Based Access Control (RBAC):
- **Superadmin:** Has full access to all tabs (Dashboard, Operations, Financial, CRM Intelligence, and Admin).
- **Admin:** Has full access to all tabs, mirroring the Superadmin.
- **Operations:** Access to the Dashboard and the Operations tools.
- **Financial:** Access to the Dashboard and the Financial metrics.

## 1. 📊 Dashboard
The Dashboard provides a top-down view of the business:
- **Operations Pulse:** Fast 90-day execution metrics.
- **System Health:** Highlights active campaigns and potential ingestion issues.

## 2. 📞 Operations Command
Designed for the Operations and Telemarketing teams.
- **Ingestion:** Navigate to the `🗄️ Operations Ingestion` tab to upload your daily `.csv` files.
- **Metrics:** Visualize true CAC, Pitch vs List Scorecards, and execution efficiency across brands.
- **Force Refresh:** If the data seems stale, use the standard `🔄 Refresh Cache` button. The Dashboard natively caches records for 24 hours to prevent extreme server loads.

## 3. 🏦 Financial Deep-Dive
Designed for the Financial Analysis team.
- **Ingestion:** Navigate to the `📥 Financial Ingestion` tab to upload the monthly `.xlsx` financial records from each brand. Multiple sheets per Excel file are fully supported.
- **Metrics:** Track GGR, Player Activity, Retained Value (LTV), and Whale dependency across all global clients.

## 4. ⚙️ Admin & Maintenance
For system administrators only:
- **User Management:** Create, edit, and delete system users and their passwords securely (SHA-256 hashed).
- **System Maintenance:** Purge local caches, manually generate benchmark snapshots, and maintain database integrity via the UI buttons.
- **File Explorer:** View any backend files live through the interface without needing SSH backend access.

## Support
Contact the IT team to add new users or resolve ingestion blocking errors. You can manually force a data refresh in the respective ingestion tabs if your uploads do not appear immediately.
