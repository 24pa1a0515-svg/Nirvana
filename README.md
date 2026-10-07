PROJECT NAME: Q-NIRVANA

PROJECT TYPE:
Healthcare + Digital Twin Platform

CHALLENGE:
Digital Twin Challenge 2026

TEAM NAME:
Q-NIRVANA

COLLEGE:
Vishnu Institute of Technology, Andhra Pradesh
Affiliated to JNTUK
Department: Computer Science and Engineering

TEAM MEMBERS:
1. Alluri Naren Varma
   Roll No: 24PA1A0515
   3rd Year, CSE

2. Bezawada Lakshith Venkat Sai
   Roll No: 24PA1A0532
   3rd Year, CSE – Section A


PROJECT OVERVIEW:

Q-NIRVANA is a healthcare platform designed to connect patients, hospitals, and ambulance operators through a unified digital system.

The project combines healthcare management, emergency response, real-time communication, ambulance GPS tracking, hospital resource management, and Digital Twin technology.

The primary Digital Twin Challenge focus is Type 2 Diabetes, where the system aims to estimate approximately 2-hour-ahead glucose-spike risk using historical healthcare information and dynamic wearable data.


PROBLEM STATEMENT:

Healthcare services often involve disconnected processes such as hospital discovery, doctor appointments, patient queues, consultations, emergency requests, ambulance coordination, and patient monitoring.

Patients may need to interact with multiple systems, while hospitals need efficient coordination of appointments, queues, emergencies, staff, resources, and ambulance services.

Q-NIRVANA aims to bring these workflows together into one connected platform.

For the Digital Twin component, the project explores how healthcare and wearable data can be combined to represent a patient's current state and estimate future glucose-spike risk.


MAIN USER ROLES:

1. PATIENT
2. HOSPITAL
3. AMBULANCE OPERATOR

Hospital users can have different designations such as:
- Doctor
- Hospital Administrator
- Receptionist
- Nurse
- Emergency Staff
- Other authorized staff

Access must be controlled according to the authenticated user's role and authorization.


PATIENT FEATURES:

- Registration and login
- Firebase authentication
- Hospital search
- Hospital details
- Department browsing
- Doctor discovery
- Appointment booking
- Appointment status
- Queue/token management
- Consultation information
- Authorized medical history
- Emergency request
- Ambulance tracking
- Notifications
- Digital Twin information where authorized


HOSPITAL FEATURES:

- Hospital authentication
- Role/designation-based dashboard
- Hospital profile
- Department management
- Staff management
- Doctor management
- Appointment management
- Patient queue management
- Token generation
- Consultation management
- Emergency management
- Ambulance coordination
- Hospital resource management
- Notifications
- Analytics
- Authorized patient information


AMBULANCE OPERATOR FEATURES:

- Registration/login
- Ambulance information
- Availability status
- Emergency request notification
- Accept emergency request
- View assigned emergency
- Hospital destination
- Live GPS location sharing
- Emergency status updates
- Arrival confirmation
- Emergency completion


APPOINTMENT SYSTEM:

The appointment workflow is:

Patient selects hospital
→ Selects department
→ Selects doctor
→ Selects available appointment
→ Sends appointment request
→ Hospital/doctor confirms
→ Patient receives confirmation
→ Patient receives queue/token information
→ Doctor calls patient
→ Consultation is completed

Possible appointment statuses:
- requested
- confirmed
- waiting
- in_progress
- completed
- cancelled

The backend must validate doctor, hospital, patient, time slot, and authorization and prevent duplicate bookings.


QUEUE SYSTEM:

The hospital can generate and manage patient tokens.

Patients can see their queue status.

Doctors can call the next patient.

Queue changes should be reflected in real time using Socket.IO.

Example:

Token 15 → Waiting
Token 14 → Called
Token 13 → Completed


EMERGENCY SYSTEM:

Emergency workflow:

Patient creates emergency request
→ System searches available registered ambulance
→ Ambulance assigned
→ Operator accepts
→ Operator travels to location
→ Live GPS updates
→ Hospital receives emergency information
→ Ambulance reaches destination
→ Hospital handles patient
→ Emergency completed

Possible statuses:
- requested
- searching
- assigned
- accepted
- en_route
- arrived
- hospital_notified
- completed
- cancelled

Important actions such as Accept, Confirm, and Complete must be validated by the backend and protected against duplicate requests.


AMBULANCE GPS TRACKING:

The ambulance operator can share:

- Latitude
- Longitude
- Timestamp

The backend stores and broadcasts location updates.

Socket.IO sends updates to authorized users.

Google Maps displays the active ambulance location.

No fake ambulance markers or fake ambulance locations should be shown when there is no registered active ambulance.


HOSPITAL RESOURCES:

Hospital resources can include:

- Beds
- ICU beds
- Emergency beds
- Ventilators
- Oxygen
- Other available resources

All resource values must come from the configured database.

Do not use fake or hardcoded hospital resource values.

If no data exists, show:

"No hospital resource data available."


DIGITAL TWIN:

The Digital Twin component focuses on:

TYPE 2 DIABETES

MAIN OBJECTIVE:

Estimate approximately 2-hour-ahead glucose-spike risk.

The Digital Twin combines historical healthcare information with dynamic wearable information.


HISTORICAL/EHR INPUTS:

- Age
- Gender
- Previous diagnoses
- Previous glucose readings
- HbA1c
- Blood pressure
- BMI
- Laboratory information
- Medical history
- Medication information where available
- Genetic information where available


WEARABLE INPUTS:

- Continuous glucose
- Heart rate
- HRV
- Steps
- Sleep
- Physical activity
- Timestamp


DIGITAL TWIN PIPELINE:

EHR Data
+
Wearable Data
↓
Data Cleaning
↓
Missing Value Handling
↓
Normalization/Scaling
↓
Feature Extraction
↓
Time-Series Analysis
↓
Digital Twin State
↓
Prediction Model
↓
Future Glucose Estimate
↓
Glucose-Spike Risk


POSSIBLE FEATURES:

- Current glucose
- Recent glucose change
- Glucose trend
- Historical glucose
- HbA1c
- Heart-rate trend
- HRV trend
- Recent physical activity
- Steps
- Sleep duration
- Recent activity level


PREDICTION OUTPUT:

The system can display:

- Current glucose
- Predicted glucose
- Prediction horizon
- Risk probability
- Risk level
- Confidence where supported
- Contributing factors
- Historical trend
- Current trend
- Digital Twin state


IMPORTANT DIGITAL TWIN REQUIREMENTS:

Do not simply hardcode:

"High Risk"

The prediction must be generated from processed input data using an actual algorithmic/statistical or trained model approach.

If a trained ML model is not available, a transparent statistical/prototype prediction method may be used.

The system must clearly state:

"SYNTHETIC DIGITAL TWIN DATA — PROTOTYPE"

and:

"NOT A MEDICAL DIAGNOSIS"

Synthetic challenge data must remain separate from real healthcare records.


REAL-TIME DIGITAL TWIN:

When wearable information changes:

Wearable Data
→ Backend
→ Digital Twin State
→ Prediction
→ Socket.IO
→ Authorized Doctor/Patient Dashboard

The dashboard should update without requiring a full page refresh.


AUTHENTICATION:

Use Firebase Authentication.

Required capabilities:

- Registration
- Login
- Logout
- Email verification
- Resend verification email
- Forgot password
- Password reset
- Authentication state
- Firebase UID
- ID token
- Token refresh

The frontend sends:

Authorization: Bearer <Firebase_ID_Token>

The backend verifies the Firebase ID token using Firebase Admin SDK.

Never trust a role, user ID, patient ID, hospital ID, or doctor ID supplied only by the frontend.


DATABASE:

Use MySQL.

Suggested tables:

- users
- patients
- hospitals
- departments
- hospital_users
- doctors
- appointments
- queue_tokens
- consultations
- emergency_cases
- ambulances
- ambulance_locations
- hospital_resources
- notifications
- digital_twin_ehr
- digital_twin_wearables
- digital_twin_predictions


NO FAKE DATA:

Do NOT create:

- Fake patients
- Fake doctors
- Fake hospitals
- Fake staff
- Fake ambulance operators
- Fake ambulances
- Fake appointments
- Fake departments
- Fake medical records
- Fake emergency cases
- Fake hospital resources
- Fake credentials
- Fake Firebase users

Real healthcare data must come from authenticated users and the configured database.

Synthetic data is permitted ONLY for the Digital Twin Challenge demonstration and must be clearly labelled.


EMPTY STATES:

When database records do not exist, show appropriate messages such as:

"No hospitals currently registered."

"No doctors currently available."

"No appointments available."

"No patient data available."

"No EHR data available."

"No wearable data available."

"Prediction unavailable."

"No ambulance currently available."


REAL-TIME COMMUNICATION:

Use Socket.IO for:

- Queue updates
- Patient called notifications
- Emergency alerts
- Ambulance status
- Ambulance GPS location
- Hospital resource updates
- Notifications
- Digital Twin wearable updates
- Digital Twin prediction updates

Use protected rooms such as:

hospital:<hospital_id>
patient:<patient_id>
doctor:<doctor_id>
emergency:<emergency_id>
digital-twin:<authorized_patient_id>

Only authorized users can join relevant rooms.


TECHNOLOGY STACK:

Frontend:
HTML
CSS
Vanilla JavaScript

Backend:
Node.js
Express.js

Database:
MySQL

Authentication:
Firebase Authentication
Firebase Admin SDK

Real-time:
Socket.IO

Maps:
Google Maps

Deployment:
Vercel-compatible architecture or another suitable production backend platform when persistent Socket.IO functionality requires it.


MAIN FILE STRUCTURE:

index.html
server.js
package.json
vercel.json

Do not create unnecessary additional backend files.

Do not use:
- Python backend
- Flask backend
- React
- Vite
- Next.js
- Supabase
- PostgreSQL

The Q-NIRVANA implementation should use Node.js + Express + MySQL + Firebase.


API REQUIREMENTS:

GET /api/health

POST /api/auth/register

GET /api/me

GET /api/hospitals

GET /api/hospitals/:id

GET /api/departments

GET /api/departments/:id

GET /api/staff

GET /api/doctors

GET /api/appointments

POST /api/appointments

PUT /api/appointments/:id

GET /api/queue

POST /api/queue

POST /api/queue/next

GET /api/consultations

POST /api/consultations

GET /api/emergency

POST /api/emergency

PUT /api/emergency/:id

GET /api/ambulances

POST /api/ambulances/location

GET /api/resources

PUT /api/resources

GET /api/notifications

GET /api/analytics

GET /api/settings


DIGITAL TWIN APIs:

GET /api/digital-twin/ehr

POST /api/digital-twin/ehr

GET /api/digital-twin/wearables

POST /api/digital-twin/wearables

GET /api/digital-twin/prediction

POST /api/digital-twin/predict


HEALTH CHECK:

GET /api/health

Must return HTTP 200:

{"status":"ok"}

No authentication should be required for this endpoint.


SECURITY:

The application must include:

- Firebase ID-token verification
- Backend authorization
- Role-based access control
- Hospital-level isolation
- Patient ownership checks
- Ambulance operator authorization
- Input validation
- Parameterized SQL queries
- Secure error handling
- No password storage
- No exposed Firebase Admin credentials
- No exposed database credentials
- No hardcoded secrets
- No unrestricted medical-data access


PROJECT VALUE:

Q-NIRVANA attempts to connect multiple healthcare workflows in one platform.

Instead of treating appointments, queues, emergency response, ambulance tracking, and patient monitoring as completely separate systems, Q-NIRVANA connects them through a common authenticated platform.

The Digital Twin component adds a predictive layer focused on Type 2 diabetes and approximately 2-hour-ahead glucose-spike risk.

The project demonstrates how:

Healthcare Data
+
Wearable Data
+
Real-Time Communication
+
Location Services
+
Digital Twin Technology
+
Predictive Analytics

can be combined into a unified healthcare technology platform.


PROJECT INSPIRATION:

The project was inspired by the need for better coordination between patients, hospitals, and emergency services and by the opportunity to explore Digital Twin technology in healthcare.

The main objective is not to replace doctors or provide medical diagnosis.

Instead, the project demonstrates how digital technologies can help organize healthcare workflows and provide data-driven insights.


KEY CHALLENGES:

1. Managing multiple user roles and permissions.
2. Designing secure healthcare-data access.
3. Connecting Firebase authentication with MySQL application data.
4. Managing appointments and preventing duplicate bookings.
5. Implementing real-time queue updates.
6. Implementing emergency workflows.
7. Tracking ambulance GPS locations in real time.
8. Integrating Google Maps.
9. Processing time-series wearable data.
10. Creating an algorithmic Digital Twin prediction pipeline.
11. Separating synthetic challenge data from real healthcare records.
12. Deploying the backend and real-time services reliably.


WHAT WE LEARNED:

Through Q-NIRVANA, we gained practical experience in:

- Full-stack development
- Healthcare application design
- REST API development
- MySQL database design
- Firebase authentication
- Role-based authorization
- Real-time Socket.IO communication
- GPS tracking
- Google Maps integration
- Digital Twin concepts
- Time-series data processing
- Predictive analytics
- Application security
- Deployment and debugging


IMPORTANT PRESENTATION MESSAGE:

Q-NIRVANA is a prototype healthcare technology platform.

The Digital Twin prediction is intended for demonstration and research purposes.

It must NOT be presented as a clinically validated medical prediction system or as a replacement for professional medical advice.

The system should clearly distinguish real authenticated healthcare records from synthetic Digital Twin challenge data.
