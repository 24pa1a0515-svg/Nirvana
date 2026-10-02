# Q-NIRVANA — Project Explanation

**Q-NIRVANA** is a healthcare platform that connects **patients, hospitals, and ambulance operators** in one system. It also includes a **Digital Twin for Type 2 Diabetes** that demonstrates how historical health records and wearable data can be used to estimate a future glucose spike.

The project has two major parts:

1. **Real-time healthcare management platform**
2. **Digital Twin healthcare prediction system**

---

## 1. Main Objective

The main objective of Q-NIRVANA is to create a connected healthcare ecosystem where:

**Patient → Hospital → Doctor → Emergency Services → Ambulance**

can communicate through a single platform.

The system allows patients to:

* Find registered hospitals
* Select departments
* Find actual registered doctors
* Book appointments
* Receive tokens
* Track their queue
* Attend consultations
* View consultation history
* Request emergency assistance
* Track an assigned ambulance

Hospitals can:

* Manage doctors and staff
* Manage departments
* Manage appointments
* Manage patient queues
* Conduct consultations
* Handle emergency cases
* Monitor hospital resources
* Coordinate ambulances
* View authorized Digital Twin information

Ambulance operators can:

* Receive emergency requests
* Accept emergencies
* Receive hospital destination
* Share live GPS location
* Update ambulance status
* Mark arrival
* Complete emergency assignments

---

# 2. Three Main Roles

There are exactly **three top-level roles**.

### 1. Patient

The patient uses the system to access healthcare services.

Typical flow:

```text
Register
   ↓
Login
   ↓
Search Hospital
   ↓
Select Department
   ↓
Select Doctor
   ↓
Book Appointment
   ↓
Receive Token
   ↓
Track Queue
   ↓
Consultation
   ↓
View History
```

---

### 2. Hospital

Doctors and hospital staff are part of the **Hospital role**.

They are differentiated using their designation.

For example:

* Doctor
* Hospital Administrator
* Receptionist
* Nurse
* Emergency Staff

A doctor can:

* View appointments
* View queue
* Call the next patient
* Access authorized patient information
* Create consultation records
* View authorized Digital Twin information

Hospital administrators can manage:

* Staff
* Departments
* Resources
* Appointments
* Emergency cases
* Ambulance coordination

---

### 3. Ambulance Operator

The ambulance operator handles emergency transportation.

Flow:

```text
Emergency Request
       ↓
Available Ambulance
       ↓
Operator Accepts
       ↓
Live GPS Tracking
       ↓
Destination Hospital
       ↓
Hospital Alert
       ↓
Ambulance Arrives
       ↓
Emergency Completed
```

---

# 3. Firebase Authentication

Q-NIRVANA uses your existing Firebase project:

**Project:** Q-NIRVANA
**Project ID:** `q-nirvana-d5620`

Firebase is responsible for **authentication and identity**.

It handles:

* Registration
* Login
* Logout
* Email verification
* Password reset
* Firebase UID
* Firebase ID tokens

The important architecture is:

```text
User
 ↓
Firebase Authentication
 ↓
Firebase UID
 ↓
Firebase ID Token
 ↓
Q-NIRVANA Backend
 ↓
MySQL
```

Firebase does **not** store the application's complete healthcare data.

MySQL stores the application data.

---

# 4. MySQL Database

MySQL is the main application database.

It stores information such as:

```text
Users
Patients
Hospitals
Departments
Hospital Staff
Doctors
Appointments
Queue Tokens
Consultations
Emergency Cases
Ambulances
Ambulance Locations
Hospital Resources
Notifications
Digital Twin Data
```

Firebase and MySQL have different responsibilities:

| System       | Responsibility              |
| ------------ | --------------------------- |
| Firebase     | Authentication              |
| MySQL        | Application/healthcare data |
| Socket.IO    | Real-time communication     |
| Google Maps  | Ambulance tracking          |
| Digital Twin | Prediction prototype        |

---

# 5. Patient Appointment System

Suppose a patient wants to visit a doctor.

The patient:

```text
Searches Hospital
      ↓
Selects Department
      ↓
Views Registered Doctors
      ↓
Selects Doctor
      ↓
Views Available Slots
      ↓
Books Appointment
```

The appointment is stored in MySQL.

The patient then receives a queue token.

For example:

```text
Your Token: 25

Current Token: 22

Patients Ahead: 3
```

When the doctor calls token 25, the patient's dashboard receives a real-time notification.

---

# 6. Real-Time Queue

Q-NIRVANA uses **Socket.IO** for real-time communication.

For example:

```text
Doctor calls Token 25
        ↓
Backend updates database
        ↓
Socket.IO event
        ↓
Patient dashboard
        ↓
"Token 25 — Please proceed"
```

The patient does not need to refresh the webpage.

Socket.IO can also handle:

* Queue updates
* Emergency alerts
* Ambulance updates
* Notifications
* Digital Twin updates
* Hospital resource updates

---

# 7. Emergency System

Emergency handling is one of the major components.

A patient creates an emergency request.

The system searches for an **available registered ambulance**.

```text
Patient
   ↓
Emergency Request
   ↓
Backend
   ↓
Available Ambulance
   ↓
Ambulance Operator
   ↓
Accept
   ↓
Live Location
   ↓
Hospital
```

The hospital receives information that an emergency ambulance is coming.

The hospital can prepare the required resources.

---

# 8. Ambulance Tracking

Google Maps is used for ambulance tracking.

The ambulance operator's device can provide:

```text
Latitude
Longitude
Timestamp
```

The information travels through:

```text
Ambulance Operator
       ↓
server.js
       ↓
MySQL
       ↓
Socket.IO
       ↓
Patient/Hospital
       ↓
Google Maps
```

The map can show:

* Current ambulance location
* Destination hospital
* Route
* Emergency location

The application should never show a fake ambulance marker when no real ambulance is active.

---

# 9. Digital Twin

This is the major innovative component of the project.

The selected use case is:

## Type 2 Diabetes

The Digital Twin represents a computational model of a patient's changing health state.

It combines two types of information.

### Historical EHR information

Examples:

* Age
* Gender
* Previous diagnoses
* Previous glucose
* HbA1c
* Blood pressure
* BMI
* Laboratory information
* Medical history
* Medication information where available
* Genetic markers where available

### Dynamic wearable information

Examples:

* Continuous glucose
* Heart rate
* HRV
* Steps
* Sleep
* Physical activity
* Timestamp

---

# 10. Digital Twin Pipeline

The complete process is:

```text
Historical EHR
       +
Wearable Data
       ↓
Data Preprocessing
       ↓
Feature Extraction
       ↓
Digital Twin State
       ↓
Prediction Model
       ↓
Future Glucose Prediction
       ↓
Risk Estimation
```

The target is approximately:

**2-hour-ahead glucose prediction / glucose-spike risk estimation.**

---

# 11. Example Digital Twin

Suppose the system receives:

```text
Current glucose: 118 mg/dL
Heart rate: 86 bpm
HRV: 42
Steps: 4200
Sleep: 6.5 hours
```

The backend processes the information.

It may calculate:

* Recent glucose trend
* Activity level
* Heart-rate trend
* HRV trend
* Historical glucose pattern
* Other available features

The prediction system then produces an estimate.

The dashboard could show:

```text
CURRENT GLUCOSE
118 mg/dL

PREDICTED GLUCOSE
154 mg/dL

PREDICTION HORIZON
~2 hours

RISK
Elevated

CONTRIBUTING FACTORS
• Recent glucose rise
• Recent activity level
• Historical glucose trend
• Sleep pattern
```

These values must come from the actual prediction pipeline rather than a hardcoded:

```text
High Risk
```

statement.

---

# 12. Synthetic Digital Twin Data

There is an important distinction.

The application must have **zero fake real-world healthcare data**.

That means no fake:

* doctors
* patients
* hospitals
* ambulances
* staff
* appointments
* medical records
* emergency cases

However, the Digital Twin Challenge permits **synthetic wearable/time-series data for the prototype demonstration**.

Therefore:

```text
REAL HEALTHCARE DATA
        ≠
SYNTHETIC DIGITAL TWIN DATA
```

Synthetic Digital Twin data must display:

> **SYNTHETIC DIGITAL TWIN DATA — PROTOTYPE**

and:

> **NOT A MEDICAL DIAGNOSIS**

This prevents synthetic demonstration data from being mistaken for an actual patient's medical record.

---

# 13. Real-Time Digital Twin

The Digital Twin should also work in real time.

For example:

```text
New wearable reading
       ↓
Backend
       ↓
Preprocessing
       ↓
Feature extraction
       ↓
Digital Twin state
       ↓
Prediction
       ↓
Socket.IO
       ↓
Doctor dashboard
```

If new glucose/HR/HRV/activity information arrives, the dashboard can update automatically.

No page refresh should be required.

---

# 14. No Fake Data Principle

This is one of the most important project requirements.

Q-NIRVANA must **not** use hardcoded healthcare records such as:

```javascript
const doctors = [
   ...
];
```

or:

```javascript
const hospitals = [
   ...
];
```

or:

```javascript
const ambulances = [
   ...
];
```

The system must obtain real-world application data from:

```text
Authenticated User
        ↓
Firebase
        ↓
Backend
        ↓
MySQL
```

If MySQL is empty, the application should say:

```text
No hospitals currently registered.
```

instead of displaying fake hospitals.

---

# 15. Security

Security is particularly important because this is a healthcare application.

The backend must verify:

```text
Firebase Token
        ↓
Firebase UID
        ↓
MySQL User
        ↓
Role
        ↓
Permission
        ↓
Requested Data
```

For example, a patient cannot simply change:

```text
patient_id=123
```

to access another patient's information.

The backend must verify that the authenticated Firebase UID actually owns that patient record.

Similarly, a hospital user from Hospital A must not access private records belonging to Hospital B.

---

# 16. One-Click Operations

Important actions should be executable with one click.

Examples:

* Accept Emergency
* Confirm Appointment
* Call Next Patient
* Confirm Arrival
* Complete Emergency
* Complete Consultation

But the button itself is not enough.

For example:

```text
Click Accept
     ↓
Backend verifies user
     ↓
Checks emergency status
     ↓
Checks operator permission
     ↓
Updates MySQL
     ↓
Emits Socket.IO event
     ↓
Updates dashboards
```

This also prevents duplicate actions.

---

# 17. Overall Architecture

The complete Q-NIRVANA architecture is:

```text
                    Q-NIRVANA
                        |
        +---------------+---------------+
        |               |               |
     PATIENT         HOSPITAL       AMBULANCE
        |               |            OPERATOR
        |               |               |
        +---------------+---------------+
                        |
                Firebase Auth
                        |
                   Firebase UID
                        |
                   ID Token
                        |
                    server.js
                        |
        +---------------+---------------+
        |               |               |
      MySQL         Socket.IO       Google Maps
        |               |               |
        |         Real-time data      GPS
        |
   Healthcare Data
        |
        +----------------------+
        |                      |
   Normal Healthcare      Digital Twin
       Records              Pipeline
                               |
                    EHR + Wearable Data
                               |
                         Feature Extraction
                               |
                         Prediction Model
                               |
                    Future Glucose Estimate
```

---

# 18. Technology Stack

| Component      | Technology                           |
| -------------- | ------------------------------------ |
| Frontend       | HTML, CSS, JavaScript                |
| Backend        | Node.js + Express                    |
| Authentication | Firebase Authentication              |
| Backend Auth   | Firebase Admin SDK                   |
| Database       | MySQL                                |
| Real-time      | Socket.IO                            |
| Maps           | Google Maps                          |
| Prediction     | Statistical/ML Digital Twin pipeline |
| Deployment     | Vercel-compatible architecture       |

---

# 19. What Makes the Project Different

The project is not just an appointment-booking application.

It combines:

### Healthcare Management

Appointments, queues, consultations and hospital management.

### Emergency Coordination

Patients, ambulances and hospitals communicate through a real-time emergency workflow.

### Live Ambulance Tracking

Google Maps and GPS provide location information.

### Real-Time Communication

Socket.IO eliminates the need for constant page refreshes.

### Digital Twin

Historical EHR + wearable data are used to model a changing health state and estimate future glucose behavior.

So the overall concept is:

> **A connected healthcare platform that combines hospital operations, emergency response, real-time communication, and a Type 2 Diabetes Digital Twin for predictive healthcare research.**

---

## Simple 30-second explanation

> **Q-NIRVANA is a real-time healthcare platform connecting patients, hospitals, and ambulance operators. Patients can find registered hospitals and doctors, book appointments, receive queue tokens, attend consultations, and request emergency ambulances. Hospitals can manage appointments, queues, consultations, emergencies, staff, and resources, while ambulance operators can accept emergencies and share live GPS locations. The platform uses Firebase for authentication, MySQL for healthcare data, Socket.IO for real-time updates, and Google Maps for ambulance tracking. Its key innovation is a Type 2 Diabetes Digital Twin that combines historical EHR information with wearable data such as glucose, heart rate, HRV, steps, sleep, and activity to estimate future glucose-spike risk. Synthetic data is used only for the clearly labelled Digital Twin prototype and is kept separate from real healthcare records.**
