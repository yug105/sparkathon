# 🌟 Worker Management System - Walmart Sustainability AI

## 🚀 Overview

The Worker Management System is a comprehensive solution for managing retail workers, sending intelligent notifications, and tracking sustainability activities. Built for the Walmart Sparkathon 2024, it integrates seamlessly with the multi-agent AI system to keep workers informed and engaged.

## ✨ Key Features

### 👥 **Worker Management**
- **Simple Registration & Login**: Easy onboarding for retail workers
- **Role-Based Access**: Manager, Supervisor, Specialist, Associate roles
- **Department Assignment**: Inventory, Pricing, Waste Management, Sustainability, General
- **Profile Management**: Complete worker profiles with activity tracking

### 📧 **Smart Email Notifications**
- **Critical Alerts**: Urgent notifications for immediate action
- **Daily Summaries**: Performance reports and sustainability metrics
- **Department-Specific Alerts**: Targeted notifications based on worker department
- **Beautiful HTML Emails**: Professional, branded email templates

### 🔔 **Notification Preferences**
- **Granular Control**: Workers can customize their notification preferences
- **Real-time Updates**: Instant preference changes
- **Smart Targeting**: Notifications sent only to relevant workers

### 📊 **Activity Tracking**
- **Login History**: Track worker engagement
- **Activity Logs**: Monitor system interactions
- **Performance Metrics**: Sustainability impact tracking

## 🛠️ Setup Instructions

### 1. **Backend Setup**

#### Install Dependencies
```bash
pip install -r requirements.txt
```

#### Environment Configuration
Create a `.env` file with the following variables:

```env
# OpenAI API Configuration
OPENAI_API_KEY=your_openai_api_key_here

# Database Configuration
NEON_DATABASE_URL=postgresql://username:password@host:port/database

# Email Configuration for Worker Notifications
SMTP_SERVER=smtp.gmail.com
SMTP_PORT=587
SMTP_USERNAME=your_email@gmail.com
SMTP_PASSWORD=your_app_password_here
FROM_EMAIL=noreply@walmart-sustainability.com
```

#### Gmail Setup (for email notifications)
1. Enable 2-factor authentication on your Gmail account
2. Generate an App Password:
   - Go to Google Account settings
   - Security → 2-Step Verification → App passwords
   - Generate password for "Mail"
3. Use the generated password as `SMTP_PASSWORD`

### 2. **Frontend Setup**

#### Install Dependencies
```bash
cd sparkathon_frontend/retail-ai-dashboard
npm install
```

#### Start Development Server
```bash
npm run dev
```

### 3. **Database Setup**

The system automatically creates the required tables on startup:
- `workers` - Worker profiles and preferences
- `worker_activities` - Activity tracking

## 🎯 Usage Guide

### **Worker Registration**

1. Navigate to the Worker Management page
2. Click "Don't have an account? Sign up"
3. Fill in the registration form:
   - **Name**: Full name
   - **Email**: Work email address
   - **Password**: Secure password (min 6 characters)
   - **Role**: Associate, Specialist, Supervisor, or Manager
   - **Department**: Choose relevant department
   - **Phone**: Optional contact number

4. Submit the form - a welcome email will be sent automatically

### **Worker Login**

1. Navigate to the Worker Management page
2. Enter email and password
3. Click "Sign In"
4. Access your personalized dashboard

### **Notification Preferences**

Workers can customize their notification preferences:

- **Email Notifications**: Master toggle for all emails
- **Critical Alerts**: Urgent notifications requiring immediate action
- **Daily Summaries**: Daily sustainability performance reports
- **Weekly Reports**: Comprehensive weekly reports
- **Inventory Alerts**: Stock level and expiry notifications
- **Pricing Alerts**: Price optimization opportunities
- **Waste Alerts**: Waste diversion and donation opportunities

### **Admin Features**

Managers and Supervisors can:
- View all workers in the system
- Monitor worker activities
- Access comprehensive analytics

## 🔧 API Endpoints

### **Worker Management**
- `POST /workers/register` - Register new worker
- `POST /workers/login` - Worker authentication
- `GET /workers/me` - Get worker profile
- `PUT /workers/notifications` - Update notification preferences
- `GET /workers/all` - Get all workers (admin only)

### **Demo Endpoints**
- `POST /demo/send-daily-summaries` - Trigger daily summary emails

## 📧 Email Templates

### **Critical Alert Email**
- Professional HTML design with Walmart branding
- Clear action items and urgency indicators
- Product details and value at risk
- Direct link to dashboard

### **Daily Summary Email**
- Sustainability metrics and achievements
- Environmental impact highlights
- Operational performance summary
- Motivational messaging

### **Welcome Email**
- Personalized welcome message
- System overview and benefits
- Dashboard access instructions
- Contact information

## 🎨 Frontend Features

### **Responsive Design**
- Mobile-friendly interface
- Dark theme optimized for retail environments
- Intuitive navigation

### **Real-time Updates**
- Live notification preference changes
- Activity tracking
- Session management

### **Security Features**
- Password hashing
- Session token management
- Role-based access control

## 🔄 Integration with AI System

The Worker Management System integrates seamlessly with the multi-agent AI system:

### **Automatic Notifications**
- **Inventory Alerts**: Triggered by shelf-life agent
- **Pricing Alerts**: Triggered by pricing agent
- **Waste Alerts**: Triggered by waste diversion agent
- **Critical Alerts**: Triggered by any high-priority system event

### **Smart Targeting**
- Notifications sent only to relevant departments
- Role-based alert filtering
- Preference-based delivery

### **Activity Tracking**
- All AI system interactions logged
- Worker engagement metrics
- Performance analytics

## 🚀 Demo Scenarios

### **Scenario 1: Critical Inventory Alert**
1. AI system detects expiring products
2. Critical alert sent to inventory workers
3. Workers receive immediate email notification
4. Dashboard shows real-time updates

### **Scenario 2: Daily Summary**
1. End of day sustainability metrics calculated
2. Daily summary sent to workers who opted in
3. Beautiful email with performance highlights
4. Motivation for continued engagement

### **Scenario 3: New Worker Onboarding**
1. Manager registers new worker
2. Welcome email sent automatically
3. Worker logs in and sets preferences
4. Worker starts receiving relevant notifications

## 🎯 Hackathon Impact

This system demonstrates:

### **Sustainability Focus**
- Worker engagement in sustainability initiatives
- Real-time environmental impact tracking
- Automated waste reduction coordination

### **Innovation**
- AI-powered notification system
- Smart targeting and personalization
- Beautiful, professional email templates

### **Practical Value**
- Immediate implementation potential
- Scalable architecture
- User-friendly interface

### **Technical Excellence**
- Modern React/TypeScript frontend
- FastAPI backend with real-time features
- PostgreSQL database with activity tracking
- Professional email system

## 🔮 Future Enhancements

- **Mobile App**: Native mobile application
- **Push Notifications**: Real-time mobile alerts
- **Advanced Analytics**: Worker performance metrics
- **Integration APIs**: Connect with existing HR systems
- **Multi-language Support**: International deployment ready

---

**Built for Walmart Sparkathon 2024 - Building a Sustainable Future** 🌱 