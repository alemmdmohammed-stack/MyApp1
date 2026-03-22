import express from "express";
import { createServer as createViteServer } from "vite";
import Database from "better-sqlite3";
import path from "path";
import { fileURLToPath } from "url";
import fs from "fs";
import { WebSocketServer, WebSocket } from "ws";
import http from "http";
import * as XLSX from 'xlsx';
import multer from 'multer';
import crypto from 'crypto';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
let db: Database.Database;
try {
  db = new Database("trend.db");
  db.pragma('journal_mode = WAL');
} catch (err: any) {
  console.error("Database initialization error:", err);
  if (err.message && err.message.includes('malformed')) {
    console.error("CRITICAL: Database 'trend.db' is malformed. Attempting recovery...");
    const backupName = `trend_malformed_${Date.now()}.db`;
    try {
      if (fs.existsSync("trend.db")) {
        fs.renameSync("trend.db", backupName);
        console.log(`Renamed malformed database to ${backupName}`);
      }
      db = new Database("trend.db");
      db.pragma('journal_mode = WAL');
      console.log("Created fresh database 'trend.db'");
    } catch (recoveryErr) {
      console.error("Failed to recover database:", recoveryErr);
      throw recoveryErr;
    }
  } else {
    throw err;
  }
}

const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ server });

const clients = new Set<WebSocket>();

const upload = multer({ dest: 'uploads/' });
const ENCRYPTION_KEY = process.env.DB_BACKUP_SECRET || 'a-very-secret-key-32-chars-long!!';
const IV_LENGTH = 16;

function encrypt(buffer: Buffer) {
  const iv = crypto.randomBytes(IV_LENGTH);
  const cipher = crypto.createCipheriv('aes-256-cbc', Buffer.from(ENCRYPTION_KEY.padEnd(32).slice(0, 32)), iv);
  const encrypted = Buffer.concat([cipher.update(buffer), cipher.final()]);
  return Buffer.concat([iv, encrypted]);
}

function decrypt(buffer: Buffer) {
  const iv = buffer.slice(0, IV_LENGTH);
  const encryptedText = buffer.slice(IV_LENGTH);
  const decipher = crypto.createDecipheriv('aes-256-cbc', Buffer.from(ENCRYPTION_KEY.padEnd(32).slice(0, 32)), iv);
  const decrypted = Buffer.concat([decipher.update(encryptedText), decipher.final()]);
  return decrypted;
}

wss.on("connection", (ws) => {
  clients.add(ws);
  ws.on("close", () => clients.delete(ws));
});

function broadcast(data: any) {
  const message = JSON.stringify(data);
  clients.forEach((client) => {
    if (client.readyState === WebSocket.OPEN) {
      client.send(message);
    }
  });
}

function createNotification(userId: number | null, title: string, message: string, type: string = "info") {
  const result = db.prepare(`
    INSERT INTO notifications (user_id, title, message, type)
    VALUES (?, ?, ?, ?)
  `).run(userId, title, message, type);
  
  const notification = {
    id: result.lastInsertRowid,
    user_id: userId,
    title,
    message,
    type,
    is_read: 0,
    created_at: new Date().toISOString()
  };

  broadcast({ type: "NOTIFICATION", payload: notification });
  return notification;
}

// --- Database Schema ---
db.exec(`
  CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    username TEXT UNIQUE,
    password TEXT,
    role TEXT, -- 'admin', 'accountant', 'engineer', 'cashier'
    status TEXT DEFAULT 'pending', -- 'pending', 'approved', 'rejected'
    email TEXT,
    profit_percentage REAL DEFAULT 50,
    phone TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
  );

  CREATE TABLE IF NOT EXISTS products (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT,
    category TEXT,
    brand TEXT,
    model TEXT,
    sku TEXT,
    barcode TEXT,
    price REAL,
    cost REAL,
    currency TEXT DEFAULT 'SAR',
    stock_quantity INTEGER DEFAULT 0,
    opening_stock INTEGER DEFAULT 0,
    min_stock_level INTEGER DEFAULT 5,
    unit TEXT DEFAULT 'قطعة',
    location TEXT,
    warehouse_id INTEGER,
    notes TEXT
  );

  CREATE TABLE IF NOT EXISTS customers (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT,
    phone TEXT,
    email TEXT,
    address TEXT,
    balance REAL DEFAULT 0,
    currency TEXT DEFAULT 'USD',
    opening_balance REAL DEFAULT 0,
    notes TEXT
  );

  CREATE TABLE IF NOT EXISTS suppliers (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT,
    contact_person TEXT,
    phone TEXT,
    email TEXT,
    balance REAL DEFAULT 0,
    currency TEXT DEFAULT 'USD',
    opening_balance REAL DEFAULT 0,
    notes TEXT
  );

  CREATE TABLE IF NOT EXISTS sales (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    customer_id INTEGER,
    customer_name TEXT,
    user_id INTEGER,
    total_amount REAL,
    paid_amount REAL DEFAULT 0,
    currency TEXT,
    discount REAL DEFAULT 0,
    payment_status TEXT, -- 'paid', 'partial', 'unpaid'
    payment_method TEXT,
    invoice_number TEXT,
    items TEXT, -- JSON string
    cash_box_id INTEGER,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
  );

  CREATE TABLE IF NOT EXISTS purchases (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    supplier_id INTEGER,
    supplier_name TEXT,
    user_id INTEGER,
    total_amount REAL,
    paid_amount REAL DEFAULT 0,
    currency TEXT,
    payment_status TEXT,
    payment_method TEXT,
    invoice_number TEXT,
    items TEXT, -- JSON string
    cash_box_id INTEGER,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
  );

  CREATE TABLE IF NOT EXISTS employees (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT,
    position TEXT,
    phone TEXT,
    email TEXT,
    salary REAL,
    currency TEXT DEFAULT 'USD',
    hire_date TEXT,
    status TEXT DEFAULT 'active',
    notes TEXT,
    commission_percentage REAL DEFAULT 0,
    balance REAL DEFAULT 0
  );

  CREATE TABLE IF NOT EXISTS salaries (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    employee_id INTEGER,
    employee_name TEXT,
    amount REAL,
    currency TEXT,
    month TEXT,
    payment_method TEXT,
    cash_box_id INTEGER,
    notes TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
  );

  CREATE TABLE IF NOT EXISTS ledger (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    date TEXT,
    type TEXT, -- 'revenue', 'expense'
    category TEXT,
    amount REAL,
    currency TEXT,
    description TEXT,
    cash_box_id INTEGER,
    user_id INTEGER,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
  );

  CREATE TABLE IF NOT EXISTS stock_adjustments (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    product_id INTEGER,
    product_name TEXT,
    type TEXT, -- 'damaged', 'correction', 'lost'
    quantity INTEGER,
    reason TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
  );

  CREATE TABLE IF NOT EXISTS warehouses (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT,
    location TEXT,
    notes TEXT
  );

  CREATE TABLE IF NOT EXISTS charity (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT,
    available_amount REAL DEFAULT 0,
    daily_amount REAL DEFAULT 0,
    outgoing_amount REAL DEFAULT 0,
    currency TEXT DEFAULT 'SAR',
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
  );

  CREATE TABLE IF NOT EXISTS maintenance (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    customer_name TEXT,
    customer_phone TEXT,
    device_model TEXT,
    imei TEXT,
    fault_description TEXT,
    symptoms TEXT,
    maintenance_type TEXT,
    cost REAL,
    currency TEXT DEFAULT 'SAR',
    status TEXT DEFAULT 'received',
    engineer_id INTEGER,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    completed_at DATETIME,
    notes TEXT,
    agreed_to_terms INTEGER DEFAULT 0
  );

  CREATE TABLE IF NOT EXISTS maintenance_parts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    maintenance_id INTEGER,
    name TEXT,
    cost REAL,
    quantity INTEGER DEFAULT 1,
    currency TEXT DEFAULT 'SAR',
    engineer_id INTEGER,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
  );

  CREATE TABLE IF NOT EXISTS maintenance_expenses (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    description TEXT,
    amount REAL,
    currency TEXT DEFAULT 'SAR',
    paid_by TEXT,
    engineer_id INTEGER,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
  );

  CREATE TABLE IF NOT EXISTS maintenance_losses (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    description TEXT,
    amount REAL,
    currency TEXT DEFAULT 'SAR',
    paid_by TEXT,
    engineer_id INTEGER,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
  );

  CREATE TABLE IF NOT EXISTS engineer_withdrawals (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    engineer_id INTEGER,
    amount REAL,
    currency TEXT DEFAULT 'SAR',
    reason TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
  );

  CREATE TABLE IF NOT EXISTS settlements (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    engineer_id INTEGER,
    amount REAL,
    currency TEXT DEFAULT 'SAR',
    status TEXT DEFAULT 'pending',
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
  );

  CREATE TABLE IF NOT EXISTS currencies (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT,
    code TEXT UNIQUE,
    symbol TEXT,
    exchange_rate REAL DEFAULT 1,
    is_base INTEGER DEFAULT 0
  );

  CREATE TABLE IF NOT EXISTS cash_boxes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT,
    balance REAL DEFAULT 0,
    currency TEXT DEFAULT 'SAR',
    notes TEXT
  );

  CREATE TABLE IF NOT EXISTS inventory_audit (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    product_id INTEGER,
    product_name TEXT,
    expected_quantity INTEGER,
    actual_quantity INTEGER,
    difference INTEGER,
    user_id INTEGER,
    notes TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
  );

  CREATE TABLE IF NOT EXISTS settings (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    currency TEXT DEFAULT 'SAR',
    base_currency_id INTEGER,
    rate_yer REAL DEFAULT 530,
    rate_sar REAL DEFAULT 3.75,
    store_name TEXT DEFAULT 'Trend',
    store_phone TEXT,
    store_address TEXT,
    logo_url TEXT,
    hide_app_link INTEGER DEFAULT 0
  );

  CREATE TABLE IF NOT EXISTS sales_returns (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    sale_id INTEGER,
    customer_id INTEGER,
    customer_name TEXT,
    user_id INTEGER,
    username TEXT,
    total_amount REAL,
    currency TEXT,
    items TEXT, -- JSON string
    reason TEXT,
    cash_box_id INTEGER,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
  );

  CREATE TABLE IF NOT EXISTS purchases_returns (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    purchase_id INTEGER,
    supplier_id INTEGER,
    supplier_name TEXT,
    user_id INTEGER,
    username TEXT,
    total_amount REAL,
    currency TEXT,
    items TEXT, -- JSON string
    reason TEXT,
    cash_box_id INTEGER,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
  );

  CREATE TABLE IF NOT EXISTS notifications (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER, -- NULL means for all users
    title TEXT,
    message TEXT,
    type TEXT, -- 'info', 'warning', 'success', 'error'
    is_read INTEGER DEFAULT 0,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
  );

  CREATE TABLE IF NOT EXISTS activity_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER,
    username TEXT,
    action TEXT,
    details TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
  );

  CREATE TABLE IF NOT EXISTS customer_payments (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    customer_id INTEGER,
    customer_name TEXT,
    amount REAL,
    currency TEXT,
    payment_method TEXT,
    cash_box_id INTEGER,
    notes TEXT,
    user_id INTEGER,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
  );

  CREATE TABLE IF NOT EXISTS supplier_payments (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    supplier_id INTEGER,
    supplier_name TEXT,
    amount REAL,
    currency TEXT,
    payment_method TEXT,
    cash_box_id INTEGER,
    notes TEXT,
    user_id INTEGER,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
  );
`);

// --- Migrations ---
const migrateTable = (tableName: string, columnName: string, columnDef: string) => {
  const info = db.prepare(`PRAGMA table_info(${tableName})`).all() as any[];
  const hasColumn = info.some(col => col.name === columnName);
  if (!hasColumn) {
    console.log(`Migrating ${tableName} table: adding ${columnName} column`);
    db.exec(`ALTER TABLE ${tableName} ADD COLUMN ${columnName} ${columnDef}`);
  }
};

migrateTable('users', 'status', "TEXT DEFAULT 'pending'");
migrateTable('users', 'profit_percentage', "REAL DEFAULT 50");
migrateTable('users', 'created_at', "DATETIME DEFAULT CURRENT_TIMESTAMP");
migrateTable('employees', 'status', "TEXT DEFAULT 'active'");
migrateTable('employees', 'balance', "REAL DEFAULT 0");
migrateTable('maintenance', 'status', "TEXT DEFAULT 'received'");
migrateTable('settlements', 'status', "TEXT DEFAULT 'pending'");
migrateTable('sales', 'cash_box_id', "INTEGER");
migrateTable('purchases', 'cash_box_id', "INTEGER");
migrateTable('ledger', 'cash_box_id', "INTEGER");
migrateTable('ledger', 'date', "TEXT");
migrateTable('ledger', 'user_id', "INTEGER");
migrateTable('maintenance', 'currency', "TEXT DEFAULT 'SAR'");
migrateTable('maintenance', 'device_color', "TEXT");
migrateTable('maintenance', 'password', "TEXT");
migrateTable('maintenance_parts', 'currency', "TEXT DEFAULT 'SAR'");
migrateTable('maintenance_expenses', 'currency', "TEXT DEFAULT 'SAR'");
migrateTable('maintenance_losses', 'currency', "TEXT DEFAULT 'SAR'");
migrateTable('engineer_withdrawals', 'currency', "TEXT DEFAULT 'SAR'");
migrateTable('settlements', 'currency', "TEXT DEFAULT 'SAR'");
migrateTable('charity', 'currency', "TEXT DEFAULT 'SAR'");
migrateTable('products', 'currency', "TEXT DEFAULT 'SAR'");
migrateTable('settings', 'background_url', 'TEXT');
migrateTable('sales_returns', 'username', 'TEXT');
migrateTable('sales_returns', 'cash_box_id', 'INTEGER');
migrateTable('purchases_returns', 'username', 'TEXT');
migrateTable('purchases_returns', 'cash_box_id', 'INTEGER');
migrateTable('users', 'phone', 'TEXT');
migrateTable('settings', 'hide_app_link', 'INTEGER DEFAULT 0');
migrateTable('users', 'email', 'TEXT');

app.use(express.json({ limit: '50mb' }));
app.use(express.urlencoded({ limit: '50mb', extended: true }));

const logActivity = (user_id: number | undefined, username: string | undefined, action: string, details: string) => {
  try {
    db.prepare("INSERT INTO activity_log (user_id, username, action, details) VALUES (?, ?, ?, ?)").run(user_id || null, username || 'System', action, details);
  } catch (err) {
    console.error("Failed to log activity:", err);
  }
};

// API routes FIRST
app.get("/api/health", (req, res) => {
  res.json({ status: "ok", timestamp: new Date().toISOString() });
});

// Logging middleware
app.use((req, res, next) => {
  console.log(`${new Date().toISOString()} - ${req.method} ${req.url}`);
  next();
});

// Seed default admin and engineer
const admin = db.prepare("SELECT * FROM users WHERE username = 'admin'").get();
if (!admin) {
  db.prepare("INSERT INTO users (username, password, role, status) VALUES (?, ?, ?, ?)").run("admin", "admin123", "admin", "approved");
}
const eng = db.prepare("SELECT * FROM users WHERE role = 'engineer'").get();
if (!eng) {
  db.prepare("INSERT INTO users (username, password, role, status, profit_percentage) VALUES (?, ?, ?, ?, ?)").run("engineer", "eng123", "engineer", "approved", 50);
}

// Seed default currencies
const sar = db.prepare("SELECT * FROM currencies WHERE code = 'SAR'").get();
if (!sar) {
  db.prepare("INSERT INTO currencies (name, code, symbol, exchange_rate, is_base) VALUES (?, ?, ?, ?, ?)").run("ريال سعودي", "SAR", "ر.س", 1, 1);
}
const usd = db.prepare("SELECT * FROM currencies WHERE code = 'USD'").get();
if (!usd) {
  db.prepare("INSERT INTO currencies (name, code, symbol, exchange_rate, is_base) VALUES (?, ?, ?, ?, ?)").run("دولار أمريكي", "USD", "$", 3.75, 0);
}
const yer = db.prepare("SELECT * FROM currencies WHERE code = 'YER'").get();
if (!yer) {
  db.prepare("INSERT INTO currencies (name, code, symbol, exchange_rate, is_base) VALUES (?, ?, ?, ?, ?)").run("ريال يمني", "YER", "﷼", 0.007, 0);
}

// Seed default cash box
const mainBox = db.prepare("SELECT * FROM cash_boxes WHERE name = 'الصندوق الرئيسي'").get();
if (!mainBox) {
  db.prepare("INSERT INTO cash_boxes (name, currency) VALUES (?, ?)").run("الصندوق الرئيسي", "SAR");
}

const settings = db.prepare("SELECT * FROM settings WHERE id = 1").get() as any;
if (!settings) {
  const sarCurrency = db.prepare("SELECT id FROM currencies WHERE code = 'SAR'").get() as any;
  db.prepare("INSERT INTO settings (id, currency, base_currency_id) VALUES (1, 'SAR', ?)").run(sarCurrency?.id);
} else if (settings.currency === 'USD') {
  // Update default to SAR if it was USD
  db.prepare("UPDATE settings SET currency = 'SAR' WHERE id = 1").run();
}

// --- Notifications ---
app.get("/api/notifications", (req, res) => {
  const { userId } = req.query;
  const notifications = db.prepare(`
    SELECT * FROM notifications 
    WHERE user_id IS NULL OR user_id = ? 
    ORDER BY created_at DESC 
    LIMIT 50
  `).all(userId);
  res.json(notifications);
});

app.post("/api/notifications/read-all", (req, res) => {
  const { userId } = req.body;
  db.prepare("UPDATE notifications SET is_read = 1 WHERE user_id IS NULL OR user_id = ?").run(userId);
  res.json({ success: true });
});

app.put("/api/notifications/:id/read", (req, res) => {
  db.prepare("UPDATE notifications SET is_read = 1 WHERE id = ?").run(req.params.id);
  res.json({ success: true });
});

// --- Auth ---
app.get("/api/health", (req, res) => {
  try {
    const count = db.prepare("SELECT COUNT(*) as count FROM users").get() as any;
    res.json({ status: "ok", users: count.count });
  } catch (e: any) {
    res.status(500).json({ status: "error", message: e.message });
  }
});

app.post("/api/login", (req, res) => {
  try {
    const { username, password } = req.body;
    if (!username || !password) {
      return res.status(400).json({ error: "يرجى إدخال اسم المستخدم وكلمة المرور" });
    }
    const user = db.prepare("SELECT id, username, role, status FROM users WHERE LOWER(username) = LOWER(?) AND password = ?").get(username, password) as any;
    if (user) {
      if (user.status === 'approved') {
        logActivity(user.id, user.username, 'تسجيل دخول', 'قام المستخدم بتسجيل الدخول إلى النظام');
        res.json(user);
      } else if (user.status === 'pending') {
        res.status(403).json({ error: "حسابك قيد المراجعة من قبل المدير" });
      } else {
        res.status(403).json({ error: "تم رفض طلب انضمامك" });
      }
    } else {
      res.status(401).json({ error: "اسم المستخدم أو كلمة المرور غير صحيحة" });
    }
  } catch (err: any) {
    console.error("Login error:", err);
    res.status(500).json({ error: "حدث خطأ في الخادم أثناء تسجيل الدخول" });
  }
});

app.post("/api/login-google", (req, res) => {
  try {
    const { email, uid, displayName } = req.body;
    if (!email) {
      return res.status(400).json({ error: "البريد الإلكتروني مطلوب" });
    }
    
    // Check if user exists by email or username (if email is not set)
    let user = db.prepare("SELECT id, username, role, status FROM users WHERE email = ? OR username = ?").get(email, email) as any;
    
    if (!user) {
      // Auto-register the owner as an admin if they don't exist
      const ownerEmails = ['mohammedalemmd@gmail.com', 'alemmdmohammed@gmail.com', 'mohhyyq1155@gmail.com'];
      if (ownerEmails.includes(email.toLowerCase())) {
        const username = displayName || email.split('@')[0];
        const result = db.prepare("INSERT INTO users (username, password, role, status, email) VALUES (?, ?, ?, ?, ?)").run(
          username,
          'google-auth', // Placeholder password
          'admin',
          'approved',
          email
        );
        user = { id: result.lastInsertRowid, username, role: 'admin', status: 'approved' };
        logActivity(user.id, user.username, 'تسجيل دخول تلقائي للمدير', 'تم تسجيل دخول المدير تلقائياً عبر جوجل');
      } else {
        return res.status(404).json({ error: "هذا الحساب غير مسجل في النظام. يرجى التواصل مع المدير." });
      }
    }

    if (user.status === 'approved') {
      // Update user with email if not already set
      db.prepare("UPDATE users SET email = ? WHERE id = ?").run(email, user.id);
      logActivity(user.id, user.username, 'تسجيل دخول جوجل', 'قام المستخدم بتسجيل الدخول باستخدام جوجل');
      res.json(user);
    } else if (user.status === 'pending') {
      res.status(403).json({ error: "حسابك قيد المراجعة من قبل المدير" });
    } else {
      res.status(403).json({ error: "تم رفض طلب انضمامك" });
    }
  } catch (err: any) {
    console.error("Google login error:", err);
    res.status(500).json({ error: "حدث خطأ في الخادم أثناء تسجيل الدخول بجوجل" });
  }
});

app.post("/api/register", (req, res) => {
  const { username, password, role, phone, email } = req.body;
  
  // Validation Algorithm
  if (!username || username.trim().length < 3) {
    return res.status(400).json({ error: "اسم المستخدم يجب أن يكون 3 أحرف على الأقل" });
  }
  if (!password || password.length < 6) {
    return res.status(400).json({ error: "كلمة المرور يجب أن تكون 6 أحرف على الأقل" });
  }
  if (!phone || phone.trim().length < 8) {
    return res.status(400).json({ error: "رقم الهاتف غير صالح" });
  }
  const validRoles = ['admin', 'accountant', 'engineer', 'cashier'];
  if (!role || !validRoles.includes(role)) {
    return res.status(400).json({ error: "نوع الحساب غير صالح" });
  }

  try {
    const result = db.prepare("INSERT INTO users (username, password, role, phone, email, status) VALUES (?, ?, ?, ?, ?, 'pending')").run(username, password, role, phone, email || null);
    createNotification(null, "طلب تسجيل جديد", `مستخدم جديد "${username}" يطلب الانضمام كـ ${role}`, "info");
    res.json({ id: result.lastInsertRowid, success: true });
  } catch (e: any) {
    if (e.message.includes('UNIQUE constraint failed')) {
      res.status(400).json({ error: "اسم المستخدم أو البريد الإلكتروني موجود مسبقاً" });
    } else {
      res.status(500).json({ error: e.message });
    }
  }
});

app.post("/api/verify-phone", (req, res) => {
  const { phone } = req.body;
  const user = db.prepare("SELECT * FROM users WHERE phone = ?").get(phone);
  if (user) {
    res.json({ success: true });
  } else {
    res.status(404).json({ error: "رقم الهاتف غير مسجل" });
  }
});

app.post("/api/reset-password", (req, res) => {
  const { phone, newPassword } = req.body;
  try {
    const result = db.prepare("UPDATE users SET password = ? WHERE phone = ?").run(newPassword, phone);
    if (result.changes > 0) {
      const user = db.prepare("SELECT * FROM users WHERE phone = ?").get(phone) as any;
      createNotification(user.id, "تغيير كلمة المرور", "تم إعادة تعيين كلمة المرور الخاصة بك عبر رقم الهاتف", "warning");
      res.json({ success: true });
    } else {
      res.status(404).json({ error: "رقم الهاتف غير مسجل" });
    }
  } catch (err: any) {
    res.status(500).json({ error: err.message });
  }
});

app.post("/api/change-password", (req, res) => {
  const { userId, currentPassword, newPassword } = req.body;
  const user = db.prepare("SELECT * FROM users WHERE id = ? AND password = ?").get(userId, currentPassword) as any;
  if (user) {
    db.prepare("UPDATE users SET password = ? WHERE id = ?").run(newPassword, userId);
    res.json({ success: true });
  } else {
    res.status(401).json({ error: "كلمة المرور الحالية غير صحيحة" });
  }
});

app.post("/api/update-username", (req, res) => {
  const { userId, newUsername } = req.body;
  try {
    db.prepare("UPDATE users SET username = ? WHERE id = ?").run(newUsername, userId);
    res.json({ success: true });
  } catch (e: any) {
    res.status(400).json({ error: "اسم المستخدم موجود مسبقاً" });
  }
});

app.post("/api/update-phone", (req, res) => {
  const { userId, newPhone } = req.body;
  try {
    db.prepare("UPDATE users SET phone = ? WHERE id = ?").run(newPhone, userId);
    res.json({ success: true });
  } catch (e: any) {
    res.status(500).json({ error: e.message });
  }
});

// Admin User Management
app.get("/api/admin/users", (req, res) => {
  const users = db.prepare("SELECT id, username, role, status, created_at FROM users WHERE role != 'admin'").all();
  res.json(users);
});

app.put("/api/admin/users/:id/status", (req, res) => {
  const { status } = req.body;
  db.prepare("UPDATE users SET status = ? WHERE id = ?").run(status, req.params.id);
  res.json({ success: true });
});

function recalculateEmployeeBalance(employeeId: number) {
  try {
    const emp = db.prepare("SELECT id, commission_percentage FROM employees WHERE id = ?").get(employeeId) as any;
    if (!emp) return;
    
    const settings = getSettings();
    
    // Calculate maintenance profit
    const maintRecords = db.prepare("SELECT cost, currency FROM maintenance WHERE engineer_id = ? AND status = 'delivered'").all(emp.id) as any[];
    const partsRecords = db.prepare("SELECT cost, quantity, currency FROM maintenance_parts WHERE engineer_id = ?").all(emp.id) as any[];
    
    let totalMaint = 0;
    maintRecords.forEach(r => totalMaint += convertToBaseHelper(r.cost, r.currency, settings));
    
    let totalParts = 0;
    partsRecords.forEach(r => totalParts += convertToBaseHelper(r.cost * r.quantity, r.currency, settings));
    
    const netProfit = totalMaint - totalParts;
    const engShare = (netProfit * (emp.commission_percentage || 50)) / 100;
    
    // Deductions
    const expenses = db.prepare("SELECT amount, currency, paid_by FROM maintenance_expenses WHERE engineer_id = ?").all(emp.id) as any[];
    const losses = db.prepare("SELECT amount, currency, paid_by FROM maintenance_losses WHERE engineer_id = ?").all(emp.id) as any[];
    const withdrawals = db.prepare("SELECT amount, currency FROM engineer_withdrawals WHERE engineer_id = ?").all(emp.id) as any[];
    const settlements = db.prepare("SELECT amount, currency FROM settlements WHERE engineer_id = ? AND status = 'approved'").all(emp.id) as any[];
    
    let totalDeductions = 0;
    expenses.forEach(r => {
      const amt = convertToBaseHelper(r.amount, r.currency, settings);
      if (r.paid_by === 'engineer') totalDeductions += amt;
      else if (r.paid_by === 'shared') totalDeductions += (amt / 2);
    });
    
    losses.forEach(r => {
      const amt = convertToBaseHelper(r.amount, r.currency, settings);
      if (r.paid_by === 'engineer') totalDeductions += amt;
      else if (r.paid_by === 'shared') totalDeductions += (amt / 2);
    });
    
    withdrawals.forEach(r => totalDeductions += convertToBaseHelper(r.amount, r.currency, settings));
    settlements.forEach(r => totalDeductions += convertToBaseHelper(r.amount, r.currency, settings));
    
    const finalBalance = engShare - totalDeductions;
    db.prepare("UPDATE employees SET balance = ? WHERE id = ?").run(finalBalance, emp.id);
    broadcast({ type: "SYNC_UPDATE", table: "employees" });
  } catch (error) {
    console.error("Error recalculating employee balance:", error);
  }
}

function recalculateCustomerBalance(customerId: any) {
  try {
    const id = Number(customerId);
    if (isNaN(id)) return;

    const settings = getSettings();
    const customer = db.prepare("SELECT opening_balance, currency FROM customers WHERE id = ?").get(id) as any;
    if (!customer) return;

    const accountCurrency = customer.currency || settings.currency || 'SAR';
    
    const sales = db.prepare("SELECT total_amount, paid_amount, currency FROM sales WHERE customer_id = ?").all(id) as any[];
    const payments = db.prepare("SELECT amount, currency FROM customer_payments WHERE customer_id = ?").all(id) as any[];
    const returns = db.prepare("SELECT total_amount, currency, cash_box_id FROM sales_returns WHERE customer_id = ?").all(id) as any[];
    
    let totalDebt = 0;
    sales.forEach(s => {
      const debt = Number(s.total_amount || 0) - Number(s.paid_amount || 0);
      totalDebt += convertCurrency(debt, s.currency || accountCurrency, accountCurrency, settings);
    });

    let totalPaid = 0;
    payments.forEach(p => {
      totalPaid += convertCurrency(Number(p.amount || 0), p.currency || accountCurrency, accountCurrency, settings);
    });

    let totalReturned = 0;
    returns.forEach(r => {
      // Only subtract from balance if it wasn't a cash refund
      if (!r.cash_box_id) {
        totalReturned += convertCurrency(Number(r.total_amount || 0), r.currency || accountCurrency, accountCurrency, settings);
      }
    });
    
    const newBalance = (Number(customer.opening_balance) || 0) + totalDebt - totalPaid - totalReturned;
    
    console.log(`Recalculating balance for customer ${id}:`, {
      opening: customer.opening_balance,
      debt: totalDebt,
      paid: totalPaid,
      returned: totalReturned,
      newBalance
    });

    db.prepare("UPDATE customers SET balance = ? WHERE id = ?").run(newBalance, id);
    broadcast({ type: "SYNC_UPDATE", table: "customers", payload: { id, balance: newBalance } });
  } catch (error) {
    console.error("Error recalculating customer balance:", error);
  }
}

function recalculateSupplierBalance(supplierId: any) {
  try {
    const id = Number(supplierId);
    if (isNaN(id)) return;

    const settings = getSettings();
    const supplier = db.prepare("SELECT opening_balance, currency FROM suppliers WHERE id = ?").get(id) as any;
    if (!supplier) return;

    const accountCurrency = supplier.currency || settings.currency || 'SAR';

    const purchases = db.prepare("SELECT total_amount, paid_amount, currency FROM purchases WHERE supplier_id = ?").all(id) as any[];
    const payments = db.prepare("SELECT amount, currency FROM supplier_payments WHERE supplier_id = ?").all(id) as any[];
    const returns = db.prepare("SELECT total_amount, currency, cash_box_id FROM purchases_returns WHERE supplier_id = ?").all(id) as any[];
    
    let totalDebt = 0;
    purchases.forEach(p => {
      const debt = Number(p.total_amount || 0) - Number(p.paid_amount || 0);
      totalDebt += convertCurrency(debt, p.currency || accountCurrency, accountCurrency, settings);
    });

    let totalPaid = 0;
    payments.forEach(pay => {
      totalPaid += convertCurrency(Number(pay.amount || 0), pay.currency || accountCurrency, accountCurrency, settings);
    });

    let totalReturned = 0;
    returns.forEach(r => {
      // Only subtract from balance if it wasn't a cash refund
      if (!r.cash_box_id) {
        totalReturned += convertCurrency(Number(r.total_amount || 0), r.currency || accountCurrency, accountCurrency, settings);
      }
    });
    
    const newBalance = (Number(supplier.opening_balance) || 0) + totalDebt - totalPaid - totalReturned;
    
    console.log(`Recalculating balance for supplier ${id}:`, {
      opening: supplier.opening_balance,
      debt: totalDebt,
      paid: totalPaid,
      returned: totalReturned,
      newBalance
    });

    db.prepare("UPDATE suppliers SET balance = ? WHERE id = ?").run(newBalance, id);
    broadcast({ type: "SYNC_UPDATE", table: "suppliers", payload: { id, balance: newBalance } });
  } catch (error) {
    console.error("Error recalculating supplier balance:", error);
  }
}

// --- Maintenance API ---
app.get("/api/maintenance", (req, res) => {
  const { engineer_id } = req.query;
  let query = "SELECT * FROM maintenance";
  let params = [];
  if (engineer_id) {
    query += " WHERE engineer_id = ?";
    params.push(engineer_id);
  }
  query += " ORDER BY created_at DESC";
  const records = db.prepare(query).all(...params);
  res.json(records);
});

app.post("/api/maintenance", (req, res) => {
  const { 
    customer_name, customer_phone, device_model, imei, device_color, password,
    fault_description, symptoms, maintenance_type, cost, currency, engineer_id, agreed_to_terms 
  } = req.body;
  const result = db.prepare(`
    INSERT INTO maintenance (
      customer_name, customer_phone, device_model, imei, device_color, password,
      fault_description, symptoms, maintenance_type, cost, currency, 
      engineer_id, agreed_to_terms
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `).run(
    customer_name, customer_phone, device_model, imei, device_color, password,
    fault_description, symptoms, maintenance_type, cost, currency || 'SAR', 
    engineer_id, agreed_to_terms ? 1 : 0
  );
  
  const jobObj = { id: result.lastInsertRowid, ...req.body, status: 'Pending', created_at: new Date().toISOString() };
  broadcast({ type: "SYNC_UPDATE", table: "maintenance", payload: jobObj });
  
  createNotification(null, "سجل صيانة جديد", `تم استلام جهاز ${device_model} من العميل ${customer_name}`, "info");
  res.json({ id: result.lastInsertRowid });
});

app.put("/api/maintenance/:id", (req, res) => {
  const { 
    customer_name, customer_phone, device_model, imei, device_color, password,
    fault_description, symptoms, maintenance_type, cost, currency, status, completed_at 
  } = req.body;
  
  const oldRecord = db.prepare("SELECT engineer_id FROM maintenance WHERE id = ?").get(req.params.id) as any;
  
  db.prepare(`
    UPDATE maintenance 
    SET customer_name = ?, customer_phone = ?, device_model = ?, imei = ?, 
        device_color = ?, password = ?,
        fault_description = ?, symptoms = ?, maintenance_type = ?, 
        cost = ?, currency = ?, status = ?, completed_at = ? 
    WHERE id = ?
  `).run(
    customer_name, customer_phone, device_model, imei, 
    device_color, password,
    fault_description, symptoms, maintenance_type, 
    cost, currency, status, completed_at, req.params.id
  );
  
  if (oldRecord?.engineer_id) {
    recalculateEmployeeBalance(oldRecord.engineer_id);
  }
  
  const jobObj = { id: req.params.id, ...req.body };
  broadcast({ type: "SYNC_UPDATE", table: "maintenance", payload: jobObj });
  
  res.json({ success: true });
});

app.post("/api/maintenance/:id/deliver", (req, res) => {
  const { id } = req.params;
  const { cash_box_id, discount, notes, user_id } = req.body;
  const settings = getSettings();

  try {
    db.transaction(() => {
      const job = db.prepare("SELECT * FROM maintenance WHERE id = ?").get(id) as any;
      if (!job) throw new Error("العملية غير موجودة");
      if (job.status === 'Delivered') throw new Error("الجهاز مسلم مسبقاً");

      const parts = db.prepare("SELECT * FROM maintenance_parts WHERE maintenance_id = ?").all() as any[];
      const partsCost = parts.reduce((acc, p) => acc + convertCurrency(p.cost, p.currency, job.currency, settings), 0);
      
      const totalCost = job.cost + partsCost - (discount || 0);
      const box = db.prepare("SELECT * FROM cash_boxes WHERE id = ?").get(cash_box_id) as any;
      if (!box) throw new Error("الصندوق غير موجود");

      const convertedTotal = convertCurrency(totalCost, job.currency, box.currency, settings);

      // Update maintenance record
      db.prepare("UPDATE maintenance SET status = 'Delivered', delivered_at = ?, updated_at = ? WHERE id = ?")
        .run(new Date().toISOString(), new Date().toISOString(), id);

      // Update cash box
      db.prepare("UPDATE cash_boxes SET balance = balance + ? WHERE id = ?").run(convertedTotal, cash_box_id);

      // Log in ledger
      db.prepare(`
        INSERT INTO ledger (date, type, category, amount, currency, description, cash_box_id, user_id)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
      `).run(
        new Date().toISOString(), 
        'income', 
        'صيانة', 
        convertedTotal, 
        box.currency, 
        `تسليم جهاز ${job.device_model} - فاتورة #${id} ${notes ? '- ' + notes : ''}`, 
        cash_box_id, 
        user_id
      );

      logActivity(user_id, undefined, 'تسليم صيانة', `تسليم جهاز ${job.device_model} بمبلغ ${totalCost} ${job.currency}`);
    })();

    broadcast({ type: "SYNC_UPDATE", table: "maintenance" });
    broadcast({ type: "SYNC_UPDATE", table: "cash_boxes" });
    broadcast({ type: "SYNC_UPDATE", table: "ledger" });
    res.json({ success: true });
  } catch (e: any) {
    res.status(400).json({ error: e.message });
  }
});

// --- Parts API ---
app.get("/api/maintenance-parts", (req, res) => {
  const { engineer_id } = req.query;
  const parts = db.prepare("SELECT * FROM maintenance_parts WHERE engineer_id = ?").all(engineer_id);
  res.json(parts);
});

app.post("/api/maintenance-parts", (req, res) => {
  const { maintenance_id, name, cost, quantity, currency, engineer_id } = req.body;
  const result = db.prepare(`
    INSERT INTO maintenance_parts (maintenance_id, name, cost, quantity, currency, engineer_id)
    VALUES (?, ?, ?, ?, ?, ?)
  `).run(maintenance_id, name, cost, quantity, currency || 'SAR', engineer_id);
  
  if (engineer_id) recalculateEmployeeBalance(engineer_id);
  
  const partObj = { id: result.lastInsertRowid, ...req.body, created_at: new Date().toISOString() };
  broadcast({ type: "SYNC_UPDATE", table: "maintenance_parts", payload: partObj });
  
  res.json({ id: result.lastInsertRowid });
});

// --- Expenses & Losses ---
app.post("/api/maintenance-expenses", (req, res) => {
  const { description, amount, currency, paid_by, engineer_id } = req.body;
  const result = db.prepare("INSERT INTO maintenance_expenses (description, amount, currency, paid_by, engineer_id) VALUES (?, ?, ?, ?, ?)").run(description, amount, currency || 'SAR', paid_by, engineer_id);
  if (engineer_id) recalculateEmployeeBalance(engineer_id);
  res.json({ id: result.lastInsertRowid });
});

app.post("/api/maintenance-losses", (req, res) => {
  const { description, amount, currency, paid_by, engineer_id } = req.body;
  const result = db.prepare("INSERT INTO maintenance_losses (description, amount, currency, paid_by, engineer_id) VALUES (?, ?, ?, ?, ?)").run(description, amount, currency || 'SAR', paid_by, engineer_id);
  if (engineer_id) recalculateEmployeeBalance(engineer_id);
  res.json({ id: result.lastInsertRowid });
});

app.post("/api/engineer-withdrawals", (req, res) => {
  const { engineer_id, amount, currency, reason } = req.body;
  const result = db.prepare("INSERT INTO engineer_withdrawals (engineer_id, amount, currency, reason) VALUES (?, ?, ?, ?)").run(engineer_id, amount, currency || 'SAR', reason);
  if (engineer_id) recalculateEmployeeBalance(engineer_id);
  res.json({ id: result.lastInsertRowid });
});

// --- Settlements ---
app.get("/api/settlements", (req, res) => {
  const { engineer_id } = req.query;
  let data;
  if (engineer_id) {
    data = db.prepare(`
      SELECT s.*, u.username as engineer_name 
      FROM settlements s 
      JOIN users u ON s.engineer_id = u.id 
      WHERE s.engineer_id = ? 
      ORDER BY s.created_at DESC
    `).all(engineer_id);
  } else {
    data = db.prepare(`
      SELECT s.*, u.username as engineer_name 
      FROM settlements s 
      JOIN users u ON s.engineer_id = u.id 
      ORDER BY s.created_at DESC
    `).all();
  }
  res.json(data);
});

app.post("/api/settlements", (req, res) => {
  const { engineer_id, amount, currency } = req.body;
  const result = db.prepare("INSERT INTO settlements (engineer_id, amount, currency) VALUES (?, ?, ?)").run(engineer_id, amount, currency || 'SAR');
  
  const engineer = db.prepare("SELECT username FROM users WHERE id = ?").get(engineer_id) as any;
  
  // Log activity
  logActivity(engineer_id, engineer?.username, 'settlement_request', `طلب توريد مبلغ ${amount} ${currency || 'SAR'}`);
  
  // Notify admin
  const admins = db.prepare("SELECT id FROM users WHERE role = 'admin'").all() as any[];
  admins.forEach(admin => {
    db.prepare("INSERT INTO notifications (user_id, title, message, type) VALUES (?, ?, ?, ?)")
      .run(admin.id, 'طلب توريد جديد', `قام المهندس بتقديم طلب توريد بمبلغ ${amount}`, 'info');
  });
  broadcast({ type: 'SYNC_UPDATE', table: 'notifications' });

  res.json({ id: result.lastInsertRowid });
});

app.put("/api/settlements/:id", (req, res) => {
  const { id } = req.params;
  const { status, admin_id } = req.body; // 'approved' or 'rejected'
  
  const settlement = db.prepare("SELECT * FROM settlements WHERE id = ?").get(id) as any;
  if (!settlement) return res.status(404).json({ error: "Settlement not found" });

  db.prepare("UPDATE settlements SET status = ? WHERE id = ?").run(status, id);

  if (settlement?.engineer_id) {
    recalculateEmployeeBalance(settlement.engineer_id);
  }

  if (status === 'approved') {
    // Create ledger entry
    db.prepare(`
      INSERT INTO ledger (type, category, amount, currency, description, cash_box_id) 
      VALUES (?, ?, ?, ?, ?, ?)
    `).run(
      'revenue', 
      'توريد مهندس', 
      settlement.amount, 
      settlement.currency, 
      `توريد مبلغ من المهندس (ID: ${settlement.engineer_id})`,
      1 // Default cash box
    );
    
    // Log activity
    logActivity(admin_id, 'Admin', 'settlement_approved', `تمت الموافقة على توريد مبلغ ${settlement.amount} من المهندس ${settlement.engineer_id}`);
    
    // Notify engineer
    db.prepare("INSERT INTO notifications (user_id, title, message, type) VALUES (?, ?, ?, ?)")
      .run(settlement.engineer_id, 'تمت الموافقة على التوريد', `تمت الموافقة على طلب التوريد الخاص بك بمبلغ ${settlement.amount}`, 'success');
  } else {
    // Log activity
    logActivity(admin_id, 'Admin', 'settlement_rejected', `تم رفض توريد مبلغ ${settlement.amount} من المهندس ${settlement.engineer_id}`);
    
    // Notify engineer
    db.prepare("INSERT INTO notifications (user_id, title, message, type) VALUES (?, ?, ?, ?)")
      .run(settlement.engineer_id, 'تم رفض طلب التوريد', `تم رفض طلب التوريد الخاص بك بمبلغ ${settlement.amount}`, 'error');
  }

  broadcast({ type: 'SYNC_UPDATE', table: 'settlements' });
  broadcast({ type: 'SYNC_UPDATE', table: 'ledger' });
  broadcast({ type: 'SYNC_UPDATE', table: 'notifications' });

  res.json({ success: true });
});

// --- Recalculation Utilities ---
app.post("/api/customers/recalculate-balances", (req, res) => {
  try {
    const customers = db.prepare("SELECT id FROM customers").all() as any[];
    for (const customer of customers) {
      recalculateCustomerBalance(customer.id);
    }
    res.json({ success: true, message: "تم إعادة حساب أرصدة العملاء بنجاح" });
  } catch (error) {
    res.status(500).json({ error: error instanceof Error ? error.message : "خطأ غير معروف" });
  }
});

app.post("/api/employees/recalculate-balances", (req, res) => {
  try {
    const employees = db.prepare("SELECT id FROM employees").all() as any[];
    for (const emp of employees) {
      recalculateEmployeeBalance(emp.id);
    }
    res.json({ success: true, message: "تم إعادة حساب أرصدة المندوبين بنجاح" });
  } catch (error) {
    res.status(500).json({ error: error instanceof Error ? error.message : "خطأ غير معروف" });
  }
});

app.post("/api/suppliers/recalculate-balances", (req, res) => {
  try {
    const suppliers = db.prepare("SELECT id FROM suppliers").all() as any[];
    for (const supplier of suppliers) {
      recalculateSupplierBalance(supplier.id);
    }
    res.json({ success: true, message: "تم إعادة حساب أرصدة الموردين بنجاح" });
  } catch (error) {
    res.status(500).json({ error: error instanceof Error ? error.message : "خطأ غير معروف" });
  }
});
app.get("/api/maintenance-summary", (req, res) => {
  const { engineer_id } = req.query;
  const user = db.prepare("SELECT profit_percentage FROM users WHERE id = ?").get(engineer_id) as any;
  const settings = db.prepare("SELECT * FROM settings WHERE id = 1").get() as any;
  
  const maintRecords = db.prepare("SELECT cost, currency FROM maintenance WHERE engineer_id = ?").all(engineer_id) as any[];
  const partsRecords = db.prepare("SELECT cost, quantity, currency FROM maintenance_parts WHERE engineer_id = ?").all(engineer_id) as any[];
  const expensesRecords = db.prepare("SELECT amount, currency, paid_by FROM maintenance_expenses WHERE engineer_id = ?").all(engineer_id) as any[];
  const lossesRecords = db.prepare("SELECT amount, currency, paid_by FROM maintenance_losses WHERE engineer_id = ?").all(engineer_id) as any[];
  const withdrawalsRecords = db.prepare("SELECT amount, currency FROM engineer_withdrawals WHERE engineer_id = ?").all(engineer_id) as any[];
  const settlementsRecords = db.prepare("SELECT amount, currency, status FROM settlements WHERE engineer_id = ?").all(engineer_id) as any[];

  const convertToBase = (amount: number, currency: string) => {
    return convertToBaseHelper(amount, currency, settings);
  };

  let maint = 0;
  maintRecords.forEach(r => maint += convertToBase(r.cost, r.currency));

  let parts = 0;
  partsRecords.forEach(r => parts += convertToBase(r.cost * r.quantity, r.currency));

  let expEng = 0;
  let expShared = 0;
  expensesRecords.forEach(r => {
    const amt = convertToBase(r.amount, r.currency);
    if (r.paid_by === 'engineer') expEng += amt;
    else if (r.paid_by === 'shared') expShared += amt;
  });

  let lossEng = 0;
  let lossShared = 0;
  lossesRecords.forEach(r => {
    const amt = convertToBase(r.amount, r.currency);
    if (r.paid_by === 'engineer') lossEng += amt;
    else if (r.paid_by === 'shared') lossShared += amt;
  });

  let withdrawals = 0;
  withdrawalsRecords.forEach(r => withdrawals += convertToBase(r.amount, r.currency));

  let settled = 0;
  settlementsRecords.forEach(r => {
    if (r.status === 'approved') settled += convertToBase(r.amount, r.currency);
  });

  const netProfit = maint - parts;
  const engShareRaw = (netProfit * (user?.profit_percentage || 50)) / 100;
  const shopShareRaw = netProfit - engShareRaw;
  
  const deductions = expEng + (expShared / 2) + lossEng + (lossShared / 2) + withdrawals;
  
  res.json({
    totalMaintenance: maint,
    totalParts: parts,
    netProfit,
    engineerShare: engShareRaw,
    shopShare: shopShareRaw,
    deductions,
    finalEngineerProfit: engShareRaw - deductions,
    amountToSettle: maint - settled
  });
});

// --- Helper Functions ---
const getSettings = () => {
  try {
    const s = db.prepare("SELECT * FROM settings WHERE id = 1").get() as any;
    return s || { rate_sar: 3.75, rate_yer: 530, currency: 'SAR' };
  } catch (e) {
    return { rate_sar: 3.75, rate_yer: 530, currency: 'SAR' };
  }
};

const convertToBaseHelper = (amount: number, currency: string, settings: any) => {
  if (!amount) return 0;
  const baseCurrency = settings.currency || 'SAR';
  const effectiveFrom = currency || baseCurrency;
  
  if (effectiveFrom === baseCurrency) return amount;
  
  // Convert to USD first (pivot)
  let amountInUSD = 0;
  if (effectiveFrom === 'USD') amountInUSD = amount;
  else if (effectiveFrom === 'SAR') amountInUSD = amount / (settings.rate_sar || 3.75);
  else if (effectiveFrom === 'YER') amountInUSD = amount / (settings.rate_yer || 530);
  else amountInUSD = amount; // Fallback

  // Convert from USD to base
  if (baseCurrency === 'USD') return amountInUSD;
  if (baseCurrency === 'SAR') return amountInUSD * (settings.rate_sar || 3.75);
  if (baseCurrency === 'YER') return amountInUSD * (settings.rate_yer || 530);
  
  return amountInUSD;
};

const convertCurrency = (amount: number, from: string, to: string, settings: any) => {
  if (!amount) return 0;
  
  const s = settings || { rate_sar: 3.75, rate_yer: 530, currency: 'SAR' };
  const fromCurrency = from || s.currency || 'SAR';
  const toCurrency = to || s.currency || 'SAR';

  if (fromCurrency === toCurrency) return amount;
  
  // Convert to USD first (pivot)
  let amountInUSD = 0;
  if (fromCurrency === 'USD') amountInUSD = amount;
  else if (fromCurrency === 'SAR') amountInUSD = amount / (Number(s.rate_sar) || 3.75);
  else if (fromCurrency === 'YER') amountInUSD = amount / (Number(s.rate_yer) || 530);
  else amountInUSD = amount; // Fallback
  
  // Convert from USD to target
  if (toCurrency === 'USD') return amountInUSD;
  if (toCurrency === 'SAR') return amountInUSD * (Number(s.rate_sar) || 3.75);
  if (toCurrency === 'YER') return amountInUSD * (Number(s.rate_yer) || 530);
  
  return amountInUSD;
};

// --- API Endpoints ---

// Stats
app.get("/api/stats", (req, res) => {
  const settings = getSettings();
  const sales = db.prepare("SELECT total_amount, paid_amount, currency FROM sales").all() as any[];
  const purchases = db.prepare("SELECT total_amount, paid_amount, currency FROM purchases").all() as any[];
  const products = db.prepare("SELECT COUNT(*) as count FROM products").get() as any;
  const users = db.prepare("SELECT COUNT(*) as count FROM users").get() as any;
  const lowStock = db.prepare("SELECT COUNT(*) as count FROM products WHERE stock_quantity <= min_stock_level").get() as any;
  const customers = db.prepare("SELECT balance, currency FROM customers").all() as any[];
  const suppliers = db.prepare("SELECT balance, currency FROM suppliers").all() as any[];
  const cashBoxes = db.prepare("SELECT balance, currency FROM cash_boxes").all() as any[];
  const maintenance = db.prepare("SELECT status, COUNT(*) as count FROM maintenance GROUP BY status").all() as any[];
  
  const totalSales = sales.reduce((acc, s) => acc + convertToBaseHelper(s.total_amount, s.currency, settings), 0);
  const totalPaidSales = sales.reduce((acc, s) => acc + convertToBaseHelper(s.paid_amount || 0, s.currency, settings), 0);
  const totalPurchases = purchases.reduce((acc, p) => acc + convertToBaseHelper(p.total_amount, p.currency, settings), 0);
  const totalPaidPurchases = purchases.reduce((acc, p) => acc + convertToBaseHelper(p.paid_amount || 0, p.currency, settings), 0);
  
  const totalCustomerDebt = customers.reduce((acc, c) => acc + convertToBaseHelper(c.balance || 0, c.currency, settings), 0);
  const totalSupplierCredit = suppliers.reduce((acc, s) => acc + convertToBaseHelper(s.balance || 0, s.currency, settings), 0);
  const totalCashInBoxes = cashBoxes.reduce((acc, b) => acc + convertToBaseHelper(b.balance || 0, b.currency, settings), 0);

  const revenue = db.prepare("SELECT amount, currency FROM ledger WHERE type = 'revenue'").all() as any[];
  const expenses = db.prepare("SELECT amount, currency FROM ledger WHERE type = 'expense'").all() as any[];
  
  const totalRevenue = revenue.reduce((acc, r) => acc + convertToBaseHelper(r.amount, r.currency, settings), 0);
  const totalExpenses = expenses.reduce((acc, e) => acc + convertToBaseHelper(e.amount, e.currency, settings), 0);
  
  const recentSales = db.prepare("SELECT * FROM sales ORDER BY created_at DESC LIMIT 5").all();

  // Sales by day (last 7 days)
  const salesByDay = db.prepare(`
    SELECT date(created_at) as date, SUM(total_amount) as total, currency
    FROM sales 
    WHERE created_at >= date('now', '-7 days')
    GROUP BY date(created_at), currency
    ORDER BY date(created_at) ASC
  `).all() as any[];

  const formattedSalesByDay = salesByDay.reduce((acc: any[], s: any) => {
    const amount = convertToBaseHelper(s.total, s.currency, settings);
    const existing = acc.find(item => item.date === s.date);
    if (existing) {
      existing.total += amount;
    } else {
      acc.push({ date: s.date, total: amount });
    }
    return acc;
  }, []);

  // Top Products (by sales count)
  const topProducts = db.prepare(`
    SELECT items FROM sales ORDER BY created_at DESC LIMIT 100
  `).all() as any[];

  const productCounts: { [key: string]: number } = {};
  topProducts.forEach(sale => {
    try {
      const items = JSON.parse(sale.items);
      items.forEach((item: any) => {
        productCounts[item.name] = (productCounts[item.name] || 0) + (item.quantity || 1);
      });
    } catch (e) {}
  });

  const sortedTopProducts = Object.entries(productCounts)
    .map(([name, count]) => ({ name, count }))
    .sort((a, b) => b.count - a.count)
    .slice(0, 5);

  res.json({
    totalSales,
    totalPaidSales,
    totalPurchases,
    totalPaidPurchases,
    totalCustomerDebt,
    totalSupplierCredit,
    totalCashInBoxes,
    maintenanceStats: maintenance,
    totalProducts: products.count || 0,
    totalUsers: users.count || 0,
    lowStock: lowStock.count || 0,
    totalRevenue,
    totalExpenses,
    recentSales,
    salesByDay: formattedSalesByDay,
    topProducts: sortedTopProducts
  });
});

// Products
app.get("/api/products", (req, res) => res.json(db.prepare("SELECT * FROM products").all()));
app.post("/api/products/bulk", (req, res) => {
  const products = req.body;
  if (!Array.isArray(products)) {
    return res.status(400).json({ error: "Invalid data format" });
  }

  const insert = db.prepare(`
    INSERT INTO products (name, category, brand, model, sku, barcode, price, cost, currency, stock_quantity, opening_stock, min_stock_level, unit, location, warehouse_id, notes)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `);

  const transaction = db.transaction((items) => {
    for (const item of items) {
      insert.run(
        item.name, item.category, item.brand, item.model, item.sku, item.barcode, 
        item.price, item.cost, item.currency || 'SAR', item.stock_quantity, 
        item.opening_stock || 0, item.min_stock_level || 5, item.unit || 'قطعة', 
        item.location, item.warehouse_id, item.notes
      );
    }
  });

  try {
    transaction(products);
    broadcast({ type: "SYNC_UPDATE", table: "products" });
    res.json({ success: true, count: products.length });
  } catch (err: any) {
    console.error("Bulk insert error:", err);
    res.status(500).json({ error: err.message });
  }
});

app.post("/api/products", (req, res) => {
  const { name, category, brand, model, sku, barcode, price, cost, currency, stock_quantity, opening_stock, min_stock_level, unit, location, warehouse_id, notes } = req.body;
  const result = db.prepare(`
    INSERT INTO products (name, category, brand, model, sku, barcode, price, cost, currency, stock_quantity, opening_stock, min_stock_level, unit, location, warehouse_id, notes)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `).run(name, category, brand, model, sku, barcode, price, cost, currency || 'SAR', stock_quantity, opening_stock, min_stock_level, unit, location, warehouse_id, notes);
  
  const productObj = { id: result.lastInsertRowid, ...req.body };
  broadcast({ type: "SYNC_UPDATE", table: "products", payload: productObj });
  
  res.json({ id: result.lastInsertRowid });
});

app.put("/api/products/:id", (req, res) => {
  const { name, category, brand, model, sku, barcode, price, cost, currency, stock_quantity, min_stock_level, unit, notes } = req.body;
  db.prepare(`
    UPDATE products 
    SET name = ?, category = ?, brand = ?, model = ?, sku = ?, barcode = ?, price = ?, cost = ?, currency = ?, stock_quantity = ?, min_stock_level = ?, unit = ?, notes = ?
    WHERE id = ?
  `).run(name, category, brand, model, sku, barcode, price, cost, currency, stock_quantity, min_stock_level, unit, notes, req.params.id);
  
  const productObj = { id: req.params.id, ...req.body };
  broadcast({ type: "SYNC_UPDATE", table: "products", payload: productObj });
  
  res.json({ success: true });
});

app.delete("/api/products/:id", (req, res) => {
  db.prepare("DELETE FROM products WHERE id = ?").run(req.params.id);
  broadcast({ type: "SYNC_DELETE", table: "products", id: req.params.id });
  res.json({ success: true });
});

// Customers
app.get("/api/customers", (req, res) => res.json(db.prepare("SELECT * FROM customers").all()));
app.post("/api/customers", (req, res) => {
  const { name, phone, email, address, opening_balance, currency, notes } = req.body;
  const result = db.prepare("INSERT INTO customers (name, phone, email, address, balance, opening_balance, currency, notes) VALUES (?, ?, ?, ?, ?, ?, ?, ?)").run(name, phone, email, address, opening_balance, opening_balance, currency, notes);
  
  const customerObj = { id: result.lastInsertRowid, ...req.body };
  broadcast({ type: "SYNC_UPDATE", table: "customers", payload: customerObj });
  
  res.json({ id: result.lastInsertRowid });
});

app.put("/api/customers/:id", (req, res) => {
  const { name, phone, email, address, balance, currency, notes } = req.body;
  db.prepare(`
    UPDATE customers 
    SET name = ?, phone = ?, email = ?, address = ?, balance = ?, currency = ?, notes = ?
    WHERE id = ?
  `).run(name, phone, email, address, balance, currency, notes, req.params.id);
  
  const customerObj = { id: req.params.id, ...req.body };
  broadcast({ type: "SYNC_UPDATE", table: "customers", payload: customerObj });
  
  res.json({ success: true });
});

app.delete("/api/customers/:id", (req, res) => {
  db.prepare("DELETE FROM customers WHERE id = ?").run(req.params.id);
  broadcast({ type: "SYNC_DELETE", table: "customers", id: req.params.id });
  res.json({ success: true });
});

// Suppliers
app.get("/api/suppliers", (req, res) => res.json(db.prepare("SELECT * FROM suppliers").all()));
app.post("/api/suppliers", (req, res) => {
  const { name, contact_person, phone, email, opening_balance, currency, notes } = req.body;
  const result = db.prepare("INSERT INTO suppliers (name, contact_person, phone, email, balance, opening_balance, currency, notes) VALUES (?, ?, ?, ?, ?, ?, ?, ?)").run(name, contact_person, phone, email, opening_balance, opening_balance, currency, notes);
  
  const supplierObj = { id: result.lastInsertRowid, ...req.body };
  broadcast({ type: "SYNC_UPDATE", table: "suppliers", payload: supplierObj });
  
  res.json({ id: result.lastInsertRowid });
});

app.put("/api/suppliers/:id", (req, res) => {
  const { name, contact_person, phone, email, balance, currency, notes } = req.body;
  db.prepare(`
    UPDATE suppliers 
    SET name = ?, contact_person = ?, phone = ?, email = ?, balance = ?, currency = ?, notes = ?
    WHERE id = ?
  `).run(name, contact_person, phone, email, balance, currency, notes, req.params.id);
  
  const supplierObj = { id: req.params.id, ...req.body };
  broadcast({ type: "SYNC_UPDATE", table: "suppliers", payload: supplierObj });
  
  res.json({ success: true });
});

app.delete("/api/suppliers/:id", (req, res) => {
  db.prepare("DELETE FROM suppliers WHERE id = ?").run(req.params.id);
  broadcast({ type: "SYNC_DELETE", table: "suppliers", id: req.params.id });
  res.json({ success: true });
});

// Sales
app.get("/api/sales", (req, res) => {
  try {
    const { customer_id } = req.query;
    let query = "SELECT * FROM sales";
    let params: any[] = [];
    
    if (customer_id) {
      query += " WHERE customer_id = ?";
      params.push(customer_id);
    }
    
    query += " ORDER BY created_at DESC";
    const sales = db.prepare(query).all(...params) as any[];
    const mapped = sales.map((s: any) => {
      try {
        return { ...s, items: typeof s.items === 'string' ? JSON.parse(s.items) : (s.items || []) };
      } catch (e) {
        console.error(`Error parsing items for sale ${s.id}:`, e);
        return { ...s, items: [] };
      }
    });
    res.json(mapped);
  } catch (err) {
    console.error("Error fetching sales:", err);
    res.status(500).json({ error: "Failed to fetch sales" });
  }
});
app.post("/api/sales", (req, res) => {
  try {
    const { customer_id, customer_name, user_id, username, total_amount, paid_amount = 0, currency, discount, payment_status, payment_method, invoice_number, items, cash_box_id } = req.body;
    const settings = getSettings();
    
    const finalPaidAmount = payment_status === 'paid' ? total_amount : (payment_status === 'unpaid' ? 0 : paid_amount);

    const result = db.prepare(`
      INSERT INTO sales (customer_id, customer_name, user_id, total_amount, paid_amount, currency, discount, payment_status, payment_method, invoice_number, items, cash_box_id)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `).run(customer_id, customer_name, user_id, total_amount, finalPaidAmount, currency, discount, payment_status, payment_method, invoice_number, JSON.stringify(items), cash_box_id);
    
    // Log activity
    logActivity(user_id, username || 'مستخدم', 'عملية بيع', `فاتورة رقم ${invoice_number} بقيمة ${total_amount} ${currency}`);

    // Add to ledger if there's a payment
    if (finalPaidAmount > 0 && cash_box_id) {
      const box = db.prepare("SELECT currency FROM cash_boxes WHERE id = ?").get(cash_box_id) as any;
      const boxCurrency = box ? box.currency : currency;
      const convertedAmount = convertCurrency(finalPaidAmount, currency, boxCurrency, settings);

      db.prepare(`
        INSERT INTO ledger (date, type, category, amount, currency, description, cash_box_id, user_id)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
      `).run(new Date().toISOString(), 'income', 'مبيعات', convertedAmount, boxCurrency, `بيع فاتورة رقم ${invoice_number}`, cash_box_id, user_id);
      
      // Update cash box balance
      db.prepare("UPDATE cash_boxes SET balance = balance + ? WHERE id = ?").run(convertedAmount, cash_box_id);
    }

    // Update stock
    if (Array.isArray(items)) {
      items.forEach((item: any) => {
        db.prepare("UPDATE products SET stock_quantity = stock_quantity - ? WHERE id = ?").run(item.quantity, item.product_id);
        
        // Check low stock
        const product = db.prepare("SELECT name, stock_quantity, min_stock_level FROM products WHERE id = ?").get(item.product_id) as any;
        if (product && product.stock_quantity <= product.min_stock_level) {
          createNotification(null, "تنبيه مخزون منخفض", `المنتج "${product.name}" وصل إلى الحد الأدنى (${product.stock_quantity})`, "warning");
        }
      });
    }

    createNotification(null, "عملية بيع جديدة", `تم بيع فاتورة بقيمة ${total_amount} ${currency} للعميل ${customer_name}`, "success");
    
    const saleObj = { id: result.lastInsertRowid, ...req.body, created_at: new Date().toISOString() };
    
    if (customer_id) recalculateCustomerBalance(customer_id);
    
    broadcast({ type: "SYNC_UPDATE", table: "sales", payload: saleObj });
    
    res.json({ id: result.lastInsertRowid });
  } catch (error: any) {
    console.error("Error in POST /api/sales:", error);
    res.status(500).json({ error: error.message });
  }
});

app.delete("/api/sales/:id", (req, res) => {
  const existingReturn = db.prepare("SELECT id FROM sales_returns WHERE sale_id = ?").get(req.params.id);
  if (existingReturn) {
    return res.status(400).json({ error: "لا يمكن حذف فاتورة لها مرتجع. قم بحذف المرتجع أولاً" });
  }

  const sale = db.prepare("SELECT * FROM sales WHERE id = ?").get(req.params.id) as any;
  if (sale) {
    const items = JSON.parse(sale.items);
    const settings = getSettings();
    
    // 1. Restore stock
    items.forEach((item: any) => {
      db.prepare("UPDATE products SET stock_quantity = stock_quantity + ? WHERE id = ?").run(item.quantity, item.product_id);
    });

    // 2. Reverse cash box balance (only the paid part was added)
    if (sale.paid_amount > 0 && sale.cash_box_id) {
      const box = db.prepare("SELECT currency FROM cash_boxes WHERE id = ?").get(sale.cash_box_id) as any;
      const boxCurrency = box ? box.currency : sale.currency;
      const convertedAmount = convertCurrency(sale.paid_amount, sale.currency, boxCurrency, settings);
      db.prepare('UPDATE cash_boxes SET balance = balance - ? WHERE id = ?').run(convertedAmount, sale.cash_box_id);
    }

    // 4. Delete ledger entries
    db.prepare("DELETE FROM ledger WHERE description LIKE ?").run(`%بيع فاتورة رقم ${sale.invoice_number}%`);

    // 5. Delete the sale
    db.prepare("DELETE FROM sales WHERE id = ?").run(req.params.id);
    
    if (sale.customer_id) recalculateCustomerBalance(sale.customer_id);
    
    broadcast({ type: "SYNC_DELETE", table: "sales", id: parseInt(req.params.id) });
    broadcast({ type: "SYNC_UPDATE", table: "products" });
    broadcast({ type: "SYNC_UPDATE", table: "customers" });
    broadcast({ type: "SYNC_UPDATE", table: "cash_boxes" });
    broadcast({ type: "SYNC_UPDATE", table: "ledger" });
  }
  res.json({ success: true });
});

// Purchases
app.get("/api/purchases", (req, res) => {
  try {
    const { supplier_id } = req.query;
    let query = "SELECT * FROM purchases";
    let params: any[] = [];
    
    if (supplier_id) {
      query += " WHERE supplier_id = ?";
      params.push(supplier_id);
    }
    
    query += " ORDER BY created_at DESC";
    const purchases = db.prepare(query).all(...params) as any[];
    const mapped = purchases.map((p: any) => {
      try {
        return { ...p, items: typeof p.items === 'string' ? JSON.parse(p.items) : (p.items || []) };
      } catch (e) {
        console.error(`Error parsing items for purchase ${p.id}:`, e);
        return { ...p, items: [] };
      }
    });
    res.json(mapped);
  } catch (err) {
    console.error("Error fetching purchases:", err);
    res.status(500).json({ error: "Failed to fetch purchases" });
  }
});
app.post("/api/purchases", (req, res) => {
  try {
    const { supplier_id, supplier_name, user_id, username, total_amount, paid_amount = 0, currency, payment_status, payment_method, invoice_number, items, cash_box_id } = req.body;
    const settings = getSettings();
    
    const finalPaidAmount = payment_status === 'paid' ? total_amount : (payment_status === 'unpaid' ? 0 : paid_amount);

    const result = db.prepare(`
      INSERT INTO purchases (supplier_id, supplier_name, user_id, total_amount, paid_amount, currency, payment_status, payment_method, invoice_number, items, cash_box_id)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `).run(supplier_id, supplier_name, user_id, total_amount, finalPaidAmount, currency, payment_status, payment_method, invoice_number, JSON.stringify(items), cash_box_id);

    // Log activity
    logActivity(user_id, username || 'مستخدم', 'عملية شراء', `فاتورة رقم ${invoice_number} بقيمة ${total_amount} ${currency}`);

    // Add to ledger if there's a payment
    if (finalPaidAmount > 0 && cash_box_id) {
      const box = db.prepare("SELECT currency FROM cash_boxes WHERE id = ?").get(cash_box_id) as any;
      const boxCurrency = box ? box.currency : currency;
      const convertedAmount = convertCurrency(finalPaidAmount, currency, boxCurrency, settings);

      db.prepare(`
        INSERT INTO ledger (date, type, category, amount, currency, description, cash_box_id, user_id)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
      `).run(new Date().toISOString(), 'expense', 'مشتريات', convertedAmount, boxCurrency, `شراء فاتورة رقم ${invoice_number}`, cash_box_id, user_id);
      
      // Update cash box balance
      db.prepare("UPDATE cash_boxes SET balance = balance - ? WHERE id = ?").run(convertedAmount, cash_box_id);
    }

    // Update stock
    if (Array.isArray(items)) {
      items.forEach((item: any) => {
        db.prepare("UPDATE products SET stock_quantity = stock_quantity + ? WHERE id = ?").run(item.quantity, item.product_id);
      });
    }

    createNotification(null, "عملية شراء جديدة", `تم شراء فاتورة بقيمة ${total_amount} ${currency} من المورد ${supplier_name}`, "info");
    
    const purchaseObj = { id: result.lastInsertRowid, ...req.body, created_at: new Date().toISOString() };
    
    if (supplier_id) recalculateSupplierBalance(supplier_id);
    
    broadcast({ type: "SYNC_UPDATE", table: "purchases", payload: purchaseObj });
    
    res.json({ id: result.lastInsertRowid });
  } catch (error: any) {
    console.error("Error in POST /api/purchases:", error);
    res.status(500).json({ error: error.message });
  }
});

app.delete("/api/purchases/:id", (req, res) => {
  const existingReturn = db.prepare("SELECT id FROM purchases_returns WHERE purchase_id = ?").get(req.params.id);
  if (existingReturn) {
    return res.status(400).json({ error: "لا يمكن حذف فاتورة لها مرتجع. قم بحذف المرتجع أولاً" });
  }

  const purchase = db.prepare("SELECT * FROM purchases WHERE id = ?").get(req.params.id) as any;
  if (purchase) {
    const items = JSON.parse(purchase.items);
    const settings = getSettings();

    // 1. Reduce stock
    items.forEach((item: any) => {
      db.prepare("UPDATE products SET stock_quantity = stock_quantity - ? WHERE id = ?").run(item.quantity, item.product_id);
    });

    // 2. Reverse supplier balance
    // (Removed manual update, recalculateSupplierBalance handles it)

    // 3. Reverse cash box balance
    if (purchase.paid_amount > 0 && purchase.cash_box_id) {
      const box = db.prepare("SELECT currency FROM cash_boxes WHERE id = ?").get(purchase.cash_box_id) as any;
      const boxCurrency = box ? box.currency : purchase.currency;
      const convertedAmount = convertCurrency(purchase.paid_amount, purchase.currency, boxCurrency, settings);
      db.prepare('UPDATE cash_boxes SET balance = balance + ? WHERE id = ?').run(convertedAmount, purchase.cash_box_id);
    }

    // 4. Delete ledger entries
    db.prepare("DELETE FROM ledger WHERE description LIKE ?").run(`%شراء فاتورة رقم ${purchase.invoice_number}%`);

    // 5. Delete the purchase
    db.prepare("DELETE FROM purchases WHERE id = ?").run(req.params.id);
    
    if (purchase.supplier_id) recalculateSupplierBalance(purchase.supplier_id);
    
    broadcast({ type: "SYNC_DELETE", table: "purchases", id: parseInt(req.params.id) });
    broadcast({ type: "SYNC_UPDATE", table: "products" });
    broadcast({ type: "SYNC_UPDATE", table: "suppliers" });
    broadcast({ type: "SYNC_UPDATE", table: "cash_boxes" });
    broadcast({ type: "SYNC_UPDATE", table: "ledger" });
  }
  res.json({ success: true });
});

// Employees
app.get("/api/employees", (req, res) => res.json(db.prepare("SELECT * FROM employees").all()));
app.post("/api/employees", (req, res) => {
  const { name, position, phone, email, salary, currency, hire_date, status, notes } = req.body;
  const result = db.prepare("INSERT INTO employees (name, position, phone, email, salary, currency, hire_date, status, notes) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)").run(name, position, phone, email, salary, currency, hire_date, status, notes);
  res.json({ id: result.lastInsertRowid });
});
app.put("/api/employees/:id", (req, res) => {
  const { name, position, phone, email, salary, currency, hire_date, status, notes } = req.body;
  db.prepare("UPDATE employees SET name = ?, position = ?, phone = ?, email = ?, salary = ?, currency = ?, hire_date = ?, status = ?, notes = ? WHERE id = ?")
    .run(name, position, phone, email, salary, currency, hire_date, status, notes, req.params.id);
  res.json({ success: true });
});
app.delete("/api/employees/:id", (req, res) => {
  db.prepare("DELETE FROM employees WHERE id = ?").run(req.params.id);
  res.json({ success: true });
});

// --- Sales History ---
// (Removed duplicate GET /api/sales)

// --- Sales Returns ---
app.get("/api/sales-returns", (req, res) => {
  const { customer_id } = req.query;
  let query = "SELECT * FROM sales_returns";
  let params: any[] = [];
  
  if (customer_id) {
    query += " WHERE customer_id = ?";
    params.push(customer_id);
  }
  
  query += " ORDER BY created_at DESC";
  const returns = db.prepare(query).all(...params).map((r: any) => ({ ...r, items: JSON.parse(r.items) }));
  res.json(returns);
});

app.post("/api/sales-returns", (req, res) => {
  const { sale_id, customer_id, customer_name, user_id, username, total_amount, currency, items, reason, cash_box_id } = req.body;
  
  // Check if sale exists and get its details
  const sale = db.prepare("SELECT * FROM sales WHERE id = ?").get(sale_id) as any;
  if (!sale) {
    return res.status(400).json({ error: "الفاتورة غير موجودة أو تم حذفها" });
  }

  // Check if already returned
  const existingReturn = db.prepare("SELECT id FROM sales_returns WHERE sale_id = ?").get(sale_id);
  if (existingReturn) {
    return res.status(400).json({ error: "هذه الفاتورة تم إرجاعها مسبقاً" });
  }

  const result = db.prepare(`
    INSERT INTO sales_returns (sale_id, customer_id, customer_name, user_id, username, total_amount, currency, items, reason, cash_box_id)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `).run(sale_id, customer_id, customer_name, user_id, username || 'مستخدم', total_amount, currency, JSON.stringify(items), reason, cash_box_id);

  // Log activity
  logActivity(user_id, username || 'مستخدم', 'مردود مبيعات', `إرجاع فاتورة رقم ${sale_id} بقيمة ${total_amount} ${currency}`);

  // Update stock: Increase stock for returned items
  items.forEach((item: any) => {
    db.prepare("UPDATE products SET stock_quantity = stock_quantity + ? WHERE id = ?").run(item.quantity, item.product_id);
  });

  // Financial reversal logic
  const settings = getSettings();
  const paidAmount = sale.paid_amount || 0;
  const totalAmount = sale.total_amount || 0;
  const unpaidAmount = totalAmount - paidAmount;

  // 1. Reduce customer balance by the unpaid portion of the returned amount
  if (customer_id && unpaidAmount > 0) {
    // If we return the whole invoice, we reduce balance by unpaidAmount
    // If we return partial (not supported yet in UI but good to be safe), we'd need more complex logic.
    // Assuming full return for now as per UI.
    db.prepare("UPDATE customers SET balance = balance - ? WHERE id = ?").run(unpaidAmount, customer_id);
  }

  // 2. Refund the paid portion from the cash box
  if (cash_box_id && paidAmount > 0) {
    const box = db.prepare("SELECT currency FROM cash_boxes WHERE id = ?").get(cash_box_id) as any;
    const boxCurrency = box ? box.currency : currency;
    const convertedRefund = convertCurrency(paidAmount, currency, boxCurrency, settings);

    db.prepare(`
      INSERT INTO ledger (date, type, category, amount, currency, description, cash_box_id, user_id)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    `).run(new Date().toISOString(), 'expense', 'مردود مبيعات', convertedRefund, boxCurrency, `إرجاع مبيعات فاتورة رقم ${sale_id} (مسترد نقدي)`, cash_box_id, user_id);
    
    // Update cash box balance
    db.prepare("UPDATE cash_boxes SET balance = balance - ? WHERE id = ?").run(convertedRefund, cash_box_id);
  }

  createNotification(null, "عملية إرجاع مبيعات", `تم إرجاع فاتورة بقيمة ${total_amount} ${currency} من العميل ${customer_name}`, "warning");
  
  const returnObj = { id: result.lastInsertRowid, ...req.body, created_at: new Date().toISOString() };
  
  if (customer_id) recalculateCustomerBalance(customer_id);
  
  broadcast({ type: "SYNC_UPDATE", table: "sales_returns", payload: returnObj });
  broadcast({ type: "SYNC_UPDATE", table: "customers", payload: { id: customer_id } });
  broadcast({ type: "SYNC_UPDATE", table: "cash_boxes", payload: { id: cash_box_id } });
  broadcast({ type: "SYNC_UPDATE", table: "products", payload: {} });
  
  res.json({ id: result.lastInsertRowid });
});

// --- Purchases History ---
// (Removed duplicate GET /api/purchases)

// --- Purchases Returns ---
app.get("/api/purchases-returns", (req, res) => {
  const { supplier_id } = req.query;
  let query = "SELECT * FROM purchases_returns";
  let params: any[] = [];
  
  if (supplier_id) {
    query += " WHERE supplier_id = ?";
    params.push(supplier_id);
  }
  
  query += " ORDER BY created_at DESC";
  const returns = db.prepare(query).all(...params).map((r: any) => ({ ...r, items: JSON.parse(r.items) }));
  res.json(returns);
});

app.post("/api/purchases-returns", (req, res) => {
  const { purchase_id, supplier_id, supplier_name, user_id, username, total_amount, currency, items, reason, cash_box_id } = req.body;
  
  // Check if purchase exists and get its details
  const purchase = db.prepare("SELECT * FROM purchases WHERE id = ?").get(purchase_id) as any;
  if (!purchase) {
    return res.status(400).json({ error: "الفاتورة غير موجودة أو تم حذفها" });
  }

  // Check if already returned
  const existingReturn = db.prepare("SELECT id FROM purchases_returns WHERE purchase_id = ?").get(purchase_id);
  if (existingReturn) {
    return res.status(400).json({ error: "هذه الفاتورة تم إرجاعها مسبقاً" });
  }

  const result = db.prepare(`
    INSERT INTO purchases_returns (purchase_id, supplier_id, supplier_name, user_id, username, total_amount, currency, items, reason, cash_box_id)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `).run(purchase_id, supplier_id, supplier_name, user_id, username || 'مستخدم', total_amount, currency, JSON.stringify(items), reason, cash_box_id);

  // Log activity
  logActivity(user_id, username || 'مستخدم', 'مردود مشتريات', `إرجاع مشتريات فاتورة رقم ${purchase_id} بقيمة ${total_amount} ${currency}`);

  // Update stock: Decrease stock for returned items
  items.forEach((item: any) => {
    db.prepare("UPDATE products SET stock_quantity = stock_quantity - ? WHERE id = ?").run(item.quantity, item.product_id);
  });

  // Financial reversal logic
  const settings = getSettings();
  const paidAmount = purchase.paid_amount || 0;
  const totalAmount = purchase.total_amount || 0;
  const unpaidAmount = totalAmount - paidAmount;

  // 2. Refund the paid portion to the cash box
  if (cash_box_id && paidAmount > 0) {
    const box = db.prepare("SELECT currency FROM cash_boxes WHERE id = ?").get(cash_box_id) as any;
    const boxCurrency = box ? box.currency : currency;
    const convertedRefund = convertCurrency(paidAmount, currency, boxCurrency, settings);

    db.prepare(`
      INSERT INTO ledger (date, type, category, amount, currency, description, cash_box_id, user_id)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    `).run(new Date().toISOString(), 'income', 'مردود مشتريات', convertedRefund, boxCurrency, `إرجاع مشتريات فاتورة رقم ${purchase_id} (مسترد نقدي)`, cash_box_id, user_id);
    
    // Update cash box balance
    db.prepare("UPDATE cash_boxes SET balance = balance + ? WHERE id = ?").run(convertedRefund, cash_box_id);
  }

  createNotification(null, "عملية إرجاع مشتريات", `تم إرجاع مشتريات بقيمة ${total_amount} ${currency} للمورد ${supplier_name}`, "warning");
  
  const returnObj = { id: result.lastInsertRowid, ...req.body, created_at: new Date().toISOString() };
  
  if (supplier_id) recalculateSupplierBalance(supplier_id);
  
  broadcast({ type: "SYNC_UPDATE", table: "purchases_returns", payload: returnObj });
  broadcast({ type: "SYNC_UPDATE", table: "suppliers", payload: { id: supplier_id } });
  broadcast({ type: "SYNC_UPDATE", table: "cash_boxes", payload: { id: cash_box_id } });
  broadcast({ type: "SYNC_UPDATE", table: "products", payload: {} });
  
  res.json({ id: result.lastInsertRowid });
});

// --- Customer Payments ---
app.get("/api/customer-payments", (req, res) => {
  const { customer_id } = req.query;
  let query = "SELECT * FROM customer_payments";
  let params: any[] = [];
  
  if (customer_id) {
    query += " WHERE customer_id = ?";
    params.push(customer_id);
  }
  
  query += " ORDER BY created_at DESC";
  const payments = db.prepare(query).all(...params);
  res.json(payments);
});

app.post("/api/customer-payments", (req, res) => {
  const { customer_id, customer_name, amount, currency, payment_method, cash_box_id, notes, user_id } = req.body;
  
  const result = db.prepare(`
    INSERT INTO customer_payments (customer_id, customer_name, amount, currency, payment_method, cash_box_id, notes, user_id)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
  `).run(customer_id, customer_name, amount, currency, payment_method, cash_box_id, notes, user_id);

  // Update cash box balance
  const settings = getSettings();
  const box = db.prepare("SELECT currency FROM cash_boxes WHERE id = ?").get(cash_box_id) as any;
  const boxCurrency = box ? box.currency : currency;
  const convertedAmount = convertCurrency(amount, currency, boxCurrency, settings);
  
  db.prepare("UPDATE cash_boxes SET balance = balance + ? WHERE id = ?").run(convertedAmount, cash_box_id);

  // Add ledger entry
  db.prepare(`
    INSERT INTO ledger (date, type, category, amount, currency, description, cash_box_id, user_id)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
  `).run(new Date().toISOString(), 'revenue', 'دفعة عميل', convertedAmount, boxCurrency, `دفعة من العميل ${customer_name}`, cash_box_id, user_id);

  logActivity(user_id, undefined, 'دفعة عميل', `استلام دفعة من العميل ${customer_name} بقيمة ${amount} ${currency}`);

  const paymentObj = { id: result.lastInsertRowid, ...req.body, created_at: new Date().toISOString() };
  
  if (customer_id) recalculateCustomerBalance(customer_id);
  
  broadcast({ type: "SYNC_UPDATE", table: "customer_payments", payload: paymentObj });
  broadcast({ type: "SYNC_UPDATE", table: "customers", payload: { id: customer_id } });
  broadcast({ type: "SYNC_UPDATE", table: "cash_boxes", payload: { id: cash_box_id } });
  broadcast({ type: "SYNC_UPDATE", table: "ledger", payload: {} });

  res.json({ id: result.lastInsertRowid });
});

app.delete("/api/customer-payments/:id", (req, res) => {
  const { id } = req.params;
  const payment = db.prepare("SELECT * FROM customer_payments WHERE id = ?").get(id) as any;
  if (!payment) return res.status(404).json({ error: "الدفعة غير موجودة" });

  // Reverse cash box balance
  const settings = getSettings();
  const box = db.prepare("SELECT currency FROM cash_boxes WHERE id = ?").get(payment.cash_box_id) as any;
  const boxCurrency = box ? box.currency : payment.currency;
  const convertedAmount = convertCurrency(payment.amount, payment.currency, boxCurrency, settings);
  
  db.prepare("UPDATE cash_boxes SET balance = balance - ? WHERE id = ?").run(convertedAmount, payment.cash_box_id);

  db.prepare("DELETE FROM customer_payments WHERE id = ?").run(id);

  if (payment.customer_id) recalculateCustomerBalance(payment.customer_id);

  logActivity(undefined, undefined, 'حذف دفعة عميل', `حذف دفعة العميل ${payment.customer_name} بقيمة ${payment.amount} ${payment.currency}`);

  broadcast({ type: "SYNC_UPDATE", table: "customer_payments", payload: { id, deleted: true } });
  broadcast({ type: "SYNC_UPDATE", table: "customers", payload: { id: payment.customer_id } });
  broadcast({ type: "SYNC_UPDATE", table: "cash_boxes", payload: { id: payment.cash_box_id } });

  res.json({ success: true });
});

// --- Supplier Payments ---
app.get("/api/supplier-payments", (req, res) => {
  const { supplier_id } = req.query;
  let query = "SELECT * FROM supplier_payments";
  let params: any[] = [];
  
  if (supplier_id) {
    query += " WHERE supplier_id = ?";
    params.push(supplier_id);
  }
  
  query += " ORDER BY created_at DESC";
  const payments = db.prepare(query).all(...params);
  res.json(payments);
});

app.post("/api/supplier-payments", (req, res) => {
  const { supplier_id, supplier_name, amount, currency, payment_method, cash_box_id, notes, user_id } = req.body;
  
  const result = db.prepare(`
    INSERT INTO supplier_payments (supplier_id, supplier_name, amount, currency, payment_method, cash_box_id, notes, user_id)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
  `).run(supplier_id, supplier_name, amount, currency, payment_method, cash_box_id, notes, user_id);

  // Update cash box balance (decrease)
  const settings = getSettings();
  const box = db.prepare("SELECT currency FROM cash_boxes WHERE id = ?").get(cash_box_id) as any;
  const boxCurrency = box ? box.currency : currency;
  const convertedAmount = convertCurrency(amount, currency, boxCurrency, settings);
  
  db.prepare("UPDATE cash_boxes SET balance = balance - ? WHERE id = ?").run(convertedAmount, cash_box_id);

  // Add ledger entry
  db.prepare(`
    INSERT INTO ledger (date, type, category, amount, currency, description, cash_box_id, user_id)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
  `).run(new Date().toISOString(), 'expense', 'دفعة مورد', convertedAmount, boxCurrency, `دفعة للمورد ${supplier_name}`, cash_box_id, user_id);

  logActivity(user_id, undefined, 'دفعة مورد', `صرف دفعة للمورد ${supplier_name} بقيمة ${amount} ${currency}`);

  const paymentObj = { id: result.lastInsertRowid, ...req.body, created_at: new Date().toISOString() };
  
  if (supplier_id) recalculateSupplierBalance(supplier_id);
  
  broadcast({ type: "SYNC_UPDATE", table: "supplier_payments", payload: paymentObj });
  broadcast({ type: "SYNC_UPDATE", table: "suppliers", payload: { id: supplier_id } });
  broadcast({ type: "SYNC_UPDATE", table: "cash_boxes", payload: { id: cash_box_id } });
  broadcast({ type: "SYNC_UPDATE", table: "ledger", payload: {} });

  res.json({ id: result.lastInsertRowid });
});

app.delete("/api/supplier-payments/:id", (req, res) => {
  const { id } = req.params;
  const payment = db.prepare("SELECT * FROM supplier_payments WHERE id = ?").get(id) as any;
  if (!payment) return res.status(404).json({ error: "الدفعة غير موجودة" });

  // Reverse cash box balance
  const settings = getSettings();
  const box = db.prepare("SELECT currency FROM cash_boxes WHERE id = ?").get(payment.cash_box_id) as any;
  const boxCurrency = box ? box.currency : payment.currency;
  const convertedAmount = convertCurrency(payment.amount, payment.currency, boxCurrency, settings);
  
  db.prepare("UPDATE cash_boxes SET balance = balance + ? WHERE id = ?").run(convertedAmount, payment.cash_box_id);

  db.prepare("DELETE FROM supplier_payments WHERE id = ?").run(id);

  if (payment.supplier_id) recalculateSupplierBalance(payment.supplier_id);

  logActivity(undefined, undefined, 'حذف دفعة مورد', `حذف دفعة المورد ${payment.supplier_name} بقيمة ${payment.amount} ${payment.currency}`);

  broadcast({ type: "SYNC_UPDATE", table: "supplier_payments", payload: { id, deleted: true } });
  broadcast({ type: "SYNC_UPDATE", table: "suppliers", payload: { id: payment.supplier_id } });
  broadcast({ type: "SYNC_UPDATE", table: "cash_boxes", payload: { id: payment.cash_box_id } });

  res.json({ success: true });
});

app.get("/api/export/excel/employees", (req, res) => {
  const employees = db.prepare("SELECT * FROM employees").all() as any[];
  const workbook = XLSX.utils.book_new();
  const worksheet = XLSX.utils.json_to_sheet(employees);
  XLSX.utils.book_append_sheet(workbook, worksheet, "Employees");
  const buffer = XLSX.write(workbook, { type: 'buffer', bookType: 'xlsx' });
  res.setHeader('Content-Disposition', 'attachment; filename=employees.xlsx');
  res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
  res.send(buffer);
});

// Salaries
app.get("/api/salaries", (req, res) => res.json(db.prepare("SELECT * FROM salaries ORDER BY created_at DESC").all()));
app.get("/api/salaries/employee/:id", (req, res) => {
  res.json(db.prepare("SELECT * FROM salaries WHERE employee_id = ? ORDER BY created_at DESC").all(req.params.id));
});
app.get("/api/employees/stats", (req, res) => {
  const stats = db.prepare(`
    SELECT 
      COUNT(*) as total_employees,
      SUM(salary) as total_monthly_salaries,
      (SELECT COUNT(*) FROM employees WHERE status = 'active') as active_employees
    FROM employees
  `).get() as any;
  res.json(stats);
});
app.post("/api/salaries", (req, res) => {
  const { employee_id, employee_name, amount, currency, month, payment_method, cash_box_id, notes } = req.body;
  const result = db.prepare("INSERT INTO salaries (employee_id, employee_name, amount, currency, month, payment_method, cash_box_id, notes) VALUES (?, ?, ?, ?, ?, ?, ?, ?)").run(employee_id, employee_name, amount, currency, month, payment_method, cash_box_id, notes);
  
  // Add to ledger as expense
  if (cash_box_id) {
    const settings = getSettings();
    const box = db.prepare("SELECT currency FROM cash_boxes WHERE id = ?").get(cash_box_id) as any;
    const boxCurrency = box ? box.currency : currency;
    const convertedAmount = convertCurrency(amount, currency, boxCurrency, settings);

    db.prepare(`
      INSERT INTO ledger (date, type, category, amount, currency, description, cash_box_id)
      VALUES (?, ?, ?, ?, ?, ?, ?)
    `).run(new Date().toISOString(), 'expense', 'رواتب', convertedAmount, boxCurrency, `راتب ${employee_name} - ${month}`, cash_box_id);
    
    // Update cash box balance
    db.prepare("UPDATE cash_boxes SET balance = balance - ? WHERE id = ?").run(convertedAmount, cash_box_id);
    broadcast({ type: "SYNC_UPDATE", table: "cash_boxes", payload: { id: cash_box_id } });
  } else {
    db.prepare("INSERT INTO ledger (date, type, category, amount, currency, description) VALUES (?, ?, ?, ?, ?, ?)").run(new Date().toISOString(), 'expense', 'رواتب', amount, currency, `راتب ${employee_name} - ${month}`);
  }
  
  broadcast({ type: "SYNC_UPDATE", table: "salaries", payload: { id: result.lastInsertRowid } });
  broadcast({ type: "SYNC_UPDATE", table: "ledger", payload: {} });
  
  res.json({ id: result.lastInsertRowid });
});

// Ledger
app.get("/api/ledger", (req, res) => res.json(db.prepare("SELECT * FROM ledger ORDER BY created_at DESC").all()));
app.post("/api/ledger", (req, res) => {
  const { date, type, category, amount, currency, description, cash_box_id, user_id } = req.body;
  const settings = getSettings();
  
  let finalAmount = amount;
  let finalCurrency = currency;

  if (cash_box_id) {
    const box = db.prepare("SELECT currency FROM cash_boxes WHERE id = ?").get(cash_box_id) as any;
    if (box && box.currency !== currency) {
      finalCurrency = box.currency;
      finalAmount = convertCurrency(amount, currency, finalCurrency, settings);
    }
    
    // Update cash box balance
    const balanceChange = type === 'income' || type === 'revenue' ? finalAmount : -finalAmount;
    db.prepare("UPDATE cash_boxes SET balance = balance + ? WHERE id = ?").run(balanceChange, cash_box_id);
  }

  const result = db.prepare("INSERT INTO ledger (date, type, category, amount, currency, description, cash_box_id, user_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?)").run(date || new Date().toISOString(), type, category, finalAmount, finalCurrency, description, cash_box_id || null, user_id || null);
  
  const ledgerObj = { id: result.lastInsertRowid, ...req.body, amount: finalAmount, currency: finalCurrency, created_at: new Date().toISOString() };
  broadcast({ type: "SYNC_UPDATE", table: "ledger", payload: ledgerObj });
  if (cash_box_id) {
    broadcast({ type: "SYNC_UPDATE", table: "cash_boxes", payload: { id: cash_box_id } });
  }
  
  res.json({ id: result.lastInsertRowid });
});

// Stock Adjustments
app.get("/api/stock-adjustments", (req, res) => res.json(db.prepare("SELECT * FROM stock_adjustments ORDER BY created_at DESC").all()));
app.post("/api/stock-adjustments", (req, res) => {
  const { product_id, type, quantity, reason } = req.body;
  const product = db.prepare("SELECT name FROM products WHERE id = ?").get(product_id) as any;
  const result = db.prepare("INSERT INTO stock_adjustments (product_id, product_name, type, quantity, reason) VALUES (?, ?, ?, ?, ?)").run(product_id, product?.name, type, quantity, reason);
  
  // Update stock
  // If type is 'increase', we add. Otherwise we subtract (damaged, lost, correction)
  const stockChange = type === 'increase' ? quantity : -quantity;
  db.prepare("UPDATE products SET stock_quantity = stock_quantity + ? WHERE id = ?").run(stockChange, product_id);
  
  res.json({ id: result.lastInsertRowid });
});

// Warehouses
app.get("/api/warehouses", (req, res) => res.json(db.prepare("SELECT * FROM warehouses").all()));
app.post("/api/warehouses", (req, res) => {
  const { name, location, notes } = req.body;
  const result = db.prepare("INSERT INTO warehouses (name, location, notes) VALUES (?, ?, ?)").run(name, location, notes);
  res.json({ id: result.lastInsertRowid });
});

// Charity
app.get("/api/charity", (req, res) => res.json(db.prepare("SELECT * FROM charity").all()));
app.post("/api/charity", (req, res) => {
  const { name, available_amount, daily_amount, outgoing_amount } = req.body;
  const result = db.prepare("INSERT INTO charity (name, available_amount, daily_amount, outgoing_amount) VALUES (?, ?, ?, ?)").run(name, available_amount, daily_amount, outgoing_amount);
  res.json({ id: result.lastInsertRowid });
});

// --- Currencies API ---
app.get("/api/currencies", (req, res) => {
  res.json(db.prepare("SELECT * FROM currencies").all());
});

app.post("/api/currencies", (req, res) => {
  const { name, code, symbol, exchange_rate, is_base } = req.body;
  
  // Validation Algorithm
  if (!name || name.length < 2) return res.status(400).json({ error: "اسم العملة يجب أن يكون حرفين على الأقل" });
  if (!code || !/^[A-Z]{3}$/.test(code)) return res.status(400).json({ error: "رمز العملة يجب أن يكون 3 أحرف كبيرة (ISO)" });
  if (!symbol) return res.status(400).json({ error: "يجب إدخال رمز العملة (مثل $ أو ر.س)" });
  if (isNaN(exchange_rate) || exchange_rate <= 0) return res.status(400).json({ error: "سعر الصرف يجب أن يكون رقماً موجباً" });

  const existing = db.prepare("SELECT id FROM currencies WHERE code = ?").get(code);
  if (existing) return res.status(400).json({ error: "رمز العملة هذا مسجل مسبقاً" });

  if (is_base) {
    db.prepare("UPDATE currencies SET is_base = 0").run();
  }
  const result = db.prepare("INSERT INTO currencies (name, code, symbol, exchange_rate, is_base) VALUES (?, ?, ?, ?, ?)").run(name, code, symbol, exchange_rate, is_base ? 1 : 0);
  res.json({ id: result.lastInsertRowid });
});

app.put("/api/currencies/:id", (req, res) => {
  const { name, code, symbol, exchange_rate, is_base } = req.body;
  
  // Validation Algorithm
  if (!name || name.length < 2) return res.status(400).json({ error: "اسم العملة يجب أن يكون حرفين على الأقل" });
  if (!code || !/^[A-Z]{3}$/.test(code)) return res.status(400).json({ error: "رمز العملة يجب أن يكون 3 أحرف كبيرة (ISO)" });
  if (!symbol) return res.status(400).json({ error: "يجب إدخال رمز العملة (مثل $ أو ر.س)" });
  if (isNaN(exchange_rate) || exchange_rate <= 0) return res.status(400).json({ error: "سعر الصرف يجب أن يكون رقماً موجباً" });

  const existing = db.prepare("SELECT id FROM currencies WHERE code = ? AND id != ?").get(code, req.params.id);
  if (existing) return res.status(400).json({ error: "رمز العملة هذا مسجل مسبقاً لعملة أخرى" });

  if (is_base) {
    db.prepare("UPDATE currencies SET is_base = 0").run();
  }
  db.prepare("UPDATE currencies SET name = ?, code = ?, symbol = ?, exchange_rate = ?, is_base = ? WHERE id = ?").run(name, code, symbol, exchange_rate, is_base ? 1 : 0, req.params.id);
  res.json({ success: true });
});

app.delete("/api/currencies/:id", (req, res) => {
  db.prepare("DELETE FROM currencies WHERE id = ?").run(req.params.id);
  res.json({ success: true });
});

// --- Cash Boxes API ---
app.get("/api/cash-boxes", (req, res) => {
  res.json(db.prepare("SELECT * FROM cash_boxes").all());
});

app.post("/api/cash-boxes/transfer", (req, res) => {
  const { from_box_id, to_box_id, amount, currency, notes, user_id } = req.body;
  const settings = getSettings();

  try {
    db.transaction(() => {
      const fromBox = db.prepare("SELECT * FROM cash_boxes WHERE id = ?").get(from_box_id) as any;
      const toBox = db.prepare("SELECT * FROM cash_boxes WHERE id = ?").get(to_box_id) as any;

      if (!fromBox || !toBox) throw new Error("الصندوق غير موجود");
      
      const convertedFrom = convertCurrency(amount, currency, fromBox.currency, settings);
      const convertedTo = convertCurrency(amount, currency, toBox.currency, settings);

      if (fromBox.balance < convertedFrom) throw new Error("رصيد الصندوق المصدر غير كافٍ");

      // Update balances
      db.prepare("UPDATE cash_boxes SET balance = balance - ? WHERE id = ?").run(convertedFrom, from_box_id);
      db.prepare("UPDATE cash_boxes SET balance = balance + ? WHERE id = ?").run(convertedTo, to_box_id);

      // Log in ledger
      db.prepare(`
        INSERT INTO ledger (date, type, category, amount, currency, description, cash_box_id, user_id)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
      `).run(new Date().toISOString(), 'expense', 'تحويل مالي', convertedFrom, fromBox.currency, `تحويل صادر إلى ${toBox.name}: ${notes}`, from_box_id, user_id);

      db.prepare(`
        INSERT INTO ledger (date, type, category, amount, currency, description, cash_box_id, user_id)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
      `).run(new Date().toISOString(), 'income', 'تحويل مالي', convertedTo, toBox.currency, `تحويل وارد من ${fromBox.name}: ${notes}`, to_box_id, user_id);

      logActivity(user_id, undefined, 'تحويل مالي', `تحويل ${amount} ${currency} من ${fromBox.name} إلى ${toBox.name}`);
    })();

    broadcast({ type: "SYNC_UPDATE", table: "cash_boxes" });
    broadcast({ type: "SYNC_UPDATE", table: "ledger" });
    res.json({ success: true });
  } catch (e: any) {
    res.status(400).json({ error: e.message });
  }
});

app.post("/api/cash-boxes", (req, res) => {
  const { name, currency, notes, balance } = req.body;
  const result = db.prepare("INSERT INTO cash_boxes (name, currency, notes, balance) VALUES (?, ?, ?, ?)").run(name, currency, notes, balance || 0);
  
  const boxObj = { id: result.lastInsertRowid, ...req.body };
  broadcast({ type: "SYNC_UPDATE", table: "cash_boxes", payload: boxObj });
  
  res.json({ id: result.lastInsertRowid });
});

app.put("/api/cash-boxes/:id", (req, res) => {
  const { name, currency, notes, balance } = req.body;
  db.prepare("UPDATE cash_boxes SET name = ?, currency = ?, notes = ?, balance = ? WHERE id = ?").run(name, currency, notes, balance, req.params.id);
  
  const boxObj = { id: req.params.id, ...req.body };
  broadcast({ type: "SYNC_UPDATE", table: "cash_boxes", payload: boxObj });
  
  res.json({ success: true });
});

app.delete("/api/cash-boxes/:id", (req, res) => {
  db.prepare("DELETE FROM cash_boxes WHERE id = ?").run(req.params.id);
  broadcast({ type: "SYNC_DELETE", table: "cash_boxes", id: req.params.id });
  res.json({ success: true });
});

// --- Inventory Audit API ---
app.get("/api/inventory-audit", (req, res) => {
  res.json(db.prepare("SELECT * FROM inventory_audit ORDER BY created_at DESC").all());
});

app.post("/api/inventory-audit", (req, res) => {
  const { product_id, product_name, expected_quantity, actual_quantity, user_id, notes } = req.body;
  const difference = actual_quantity - expected_quantity;
  const result = db.prepare(`
    INSERT INTO inventory_audit (product_id, product_name, expected_quantity, actual_quantity, difference, user_id, notes)
    VALUES (?, ?, ?, ?, ?, ?, ?)
  `).run(product_id, product_name, expected_quantity, actual_quantity, difference, user_id, notes);
  
  // Optionally update product stock to match actual quantity
  db.prepare("UPDATE products SET stock_quantity = ? WHERE id = ?").run(actual_quantity, product_id);
  
  const auditObj = { id: result.lastInsertRowid, ...req.body, difference, created_at: new Date().toISOString() };
  broadcast({ type: "SYNC_UPDATE", table: "inventory_audit", payload: auditObj });
  
  res.json({ id: result.lastInsertRowid });
});

// Settings
app.get("/api/settings", (req, res) => res.json(db.prepare("SELECT * FROM settings WHERE id = 1").get()));
app.post("/api/settings", (req, res) => {
  const { currency, base_currency_id, rate_yer, rate_sar, store_name, store_phone, store_address, logo_url, background_url, hide_app_link } = req.body;
  db.prepare(`
    UPDATE settings 
    SET currency = ?, base_currency_id = ?, rate_yer = ?, rate_sar = ?, store_name = ?, store_phone = ?, store_address = ?, logo_url = ?, background_url = ?, hide_app_link = ?
    WHERE id = 1
  `).run(currency, base_currency_id, rate_yer, rate_sar, store_name, store_phone, store_address, logo_url, background_url, hide_app_link ? 1 : 0);
  res.json({ success: true });
});

// --- Excel Import/Export API ---
app.get("/api/export/excel/:table", (req, res) => {
  const table = req.params.table;
  const allowedTables = ['products', 'customers', 'suppliers'];
  if (!allowedTables.includes(table)) return res.status(400).json({ error: 'Invalid table' });

  const data = db.prepare(`SELECT * FROM ${table}`).all();
  const worksheet = XLSX.utils.json_to_sheet(data);
  const workbook = XLSX.utils.book_new();
  XLSX.utils.book_append_sheet(workbook, worksheet, table);
  const buffer = XLSX.write(workbook, { type: 'buffer', bookType: 'xlsx' });

  res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
  res.setHeader('Content-Disposition', `attachment; filename=${table}.xlsx`);
  res.send(buffer);
});

app.post("/api/import/excel/:table", upload.single('file'), (req: any, res) => {
  const table = req.params.table;
  const allowedTables = ['products', 'customers', 'suppliers'];
  if (!allowedTables.includes(table) || !req.file) return res.status(400).json({ error: 'Invalid request' });

  try {
    const workbook = XLSX.readFile(req.file.path);
    const sheetName = workbook.SheetNames[0];
    const data = XLSX.utils.sheet_to_json(workbook.Sheets[sheetName]);

    const insert = (row: any) => {
      const keys = Object.keys(row).filter(k => k !== 'id');
      const placeholders = keys.map(() => '?').join(',');
      const values = keys.map(k => row[k]);
      db.prepare(`INSERT INTO ${table} (${keys.join(',')}) VALUES (${placeholders})`).run(...values);
    };

    data.forEach(insert);
    fs.unlinkSync(req.file.path);
    res.json({ success: true, count: data.length });
  } catch (e: any) {
    if (req.file) fs.unlinkSync(req.file.path);
    res.status(500).json({ error: e.message });
  }
});

// --- Database Backup/Restore API ---
app.get("/api/database/export", (req, res) => {
  const dbPath = path.resolve(__dirname, "trend.db");
  const dbBuffer = fs.readFileSync(dbPath);
  const encrypted = encrypt(dbBuffer);
  res.setHeader('Content-Type', 'application/octet-stream');
  res.setHeader('Content-Disposition', 'attachment; filename=database_backup.enc');
  res.send(encrypted);
});

app.post("/api/database/import", upload.single('file'), (req: any, res) => {
  if (!req.file) return res.status(400).json({ error: 'No file' });
  const mode = req.body.mode || 'overwrite'; // 'overwrite' or 'merge'
  
  try {
    const encrypted = fs.readFileSync(req.file.path);
    const decrypted = decrypt(encrypted);
    const dbPath = path.resolve(__dirname, "trend.db");
    const tempDbPath = path.resolve(__dirname, "temp_restore.db");
    
    if (mode === 'overwrite') {
      db.close();
      fs.writeFileSync(dbPath, decrypted);
      res.json({ success: true, message: 'Database restored (Overwritten). Application will restart.' });
      setTimeout(() => process.exit(0), 1000);
    } else {
      // Merge mode
      fs.writeFileSync(tempDbPath, decrypted);
      const tempDb = new Database(tempDbPath);
      
      // Get all tables from the uploaded database
      const tables = tempDb.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'").all() as {name: string}[];
      
      db.transaction(() => {
        for (const { name } of tables) {
          try {
            // Attach temp database and insert or ignore
            db.prepare(`ATTACH DATABASE ? AS to_merge`).run(tempDbPath);
            db.prepare(`INSERT OR IGNORE INTO main.${name} SELECT * FROM to_merge.${name}`).run();
            db.prepare(`DETACH DATABASE to_merge`).run();
          } catch (err) {
            console.error(`Error merging table ${name}:`, err);
          }
        }
      })();
      
      tempDb.close();
      fs.unlinkSync(tempDbPath);
      fs.unlinkSync(req.file.path);
      res.json({ success: true, message: 'Database merged successfully.' });
    }
  } catch (e: any) {
    if (req.file && fs.existsSync(req.file.path)) fs.unlinkSync(req.file.path);
    res.status(500).json({ error: 'Failed to decrypt or restore database: ' + e.message });
  }
});

app.get("/api/activity-log", (req, res) => {
  res.json(db.prepare("SELECT * FROM activity_log ORDER BY created_at DESC LIMIT 500").all());
});

app.post("/api/sync/:table", (req, res) => {
  const { table } = req.params;
  const payload = req.body;
  
  if (!payload) return res.status(400).json({ error: 'Invalid data' });
  
  const items = Array.isArray(payload) ? payload : [payload];
  if (items.length === 0) return res.json({ success: true, message: 'No items to sync' });

  try {
    // Check if table exists
    const tableExists = db.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name=?").get(table);
    if (!tableExists) return res.status(404).json({ error: 'Table not found' });

    // Get table columns to filter out invalid keys
    const columns = db.prepare(`PRAGMA table_info(${table})`).all() as { name: string }[];
    const validKeys = columns.map(c => c.name);

    // Use a transaction for batch updates
    const syncTransaction = db.transaction((itemsToSync) => {
      for (const data of itemsToSync) {
        if (data.id === undefined) continue;

        const filteredKeys = Object.keys(data).filter(k => k !== 'lastUpdated' && validKeys.includes(k));
        if (filteredKeys.length === 0) continue;

        const placeholders = filteredKeys.map(() => '?').join(',');
        const values = filteredKeys.map(k => {
          let val = data[k];
          if (typeof val === 'boolean') return val ? 1 : 0;
          return (typeof val === 'object' && val !== null) ? JSON.stringify(val) : val;
        });

        db.prepare(`INSERT OR REPLACE INTO ${table} (${filteredKeys.join(',')}) VALUES (${placeholders})`).run(...values);
        
        // If it's a notification, broadcast it via WebSocket
        if (table === 'notifications') {
          broadcast({ type: "NOTIFICATION", payload: data });
        }
      }
    });

    try {
      syncTransaction(items);
      res.json({ success: true, count: items.length });
    } catch (dbError: any) {
      console.error(`Database error during sync for table ${table}:`, dbError.message);
      res.status(500).json({ error: dbError.message });
    }
  } catch (e: any) {
    console.error(`Sync API error for ${table}:`, e);
    res.status(500).json({ error: e.message });
  }
});

// --- Vite Middleware ---
process.on('uncaughtException', (err) => {
  console.error('Uncaught Exception:', err);
});

process.on('unhandledRejection', (reason, promise) => {
  console.error('Unhandled Rejection at:', promise, 'reason:', reason);
});

if (process.env.NODE_ENV !== "production") {
  const vite = await createViteServer({
    server: { middlewareMode: true },
    appType: "spa",
  });
  app.use(vite.middlewares);
} else {
  const distPath = path.join(__dirname, "dist");
  app.use(express.static(distPath));
  app.get("*", (req, res) => res.sendFile(path.join(distPath, "index.html")));
}

// Error handling middleware (MUST be last)
app.use((err: any, req: any, res: any, next: any) => {
  console.error('Server Error:', err);
  res.status(500).json({ error: 'Internal Server Error', details: err.message });
});

server.listen(3000, "0.0.0.0", () => console.log("Server running on port 3000"));
