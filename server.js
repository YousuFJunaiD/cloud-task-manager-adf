require("dotenv").config();

const express = require("express");
const cors = require("cors");
const fs = require("fs");
const path = require("path");
const { BlobServiceClient } = require("@azure/storage-blob");

const app = express();
const PORT = 3000;

app.use(cors());
app.use(express.json());

// =====================
// Azure upload helper
// =====================
async function uploadToAzure(localFilePath, blobPath) {
    const blobServiceClient = new BlobServiceClient(
        `${process.env.AZURE_BLOB_CONTAINER_URL}${process.env.AZURE_SAS_TOKEN}`
    );

    const containerClient = blobServiceClient.getContainerClient();
    const blockBlobClient = containerClient.getBlockBlobClient(blobPath);

    await blockBlobClient.uploadFile(localFilePath);
    console.log(`✅ Uploaded to Azure: ${blobPath}`);
}

// =====================
// In-memory storage
// =====================
let tasks = [];
let taskEvents = [];

let nextTaskId = 1;
let nextEventId = 1;

// =====================
// Home
// =====================
app.get("/", (req, res) => {
    res.send("Task Manager API is running");
});

// =====================
// GET all tasks
// =====================
app.get("/tasks", (req, res) => {
    res.json(tasks);
});

// =====================
// CREATE task
// =====================
app.post("/tasks", (req, res) => {
    const { title, description } = req.body;
    const now = new Date().toISOString();

    const task = {
        id: nextTaskId++,
        title,
        description,
        completed: false,
        created_at: now,
        updated_at: now
    };

    tasks.push(task);

    taskEvents.push({
        event_id: nextEventId++,
        task_id: task.id,
        event_type: "task_created",
        event_time: now
    });

    res.json(task);
});

// =====================
// UPDATE task
// =====================
app.put("/tasks/:id", (req, res) => {
    const id = parseInt(req.params.id);
    const { completed } = req.body;
    const now = new Date().toISOString();

    const task = tasks.find(t => t.id === id);
    if (!task) {
        return res.status(404).json({ message: "Task not found" });
    }

    task.completed = completed;
    task.updated_at = now;

    taskEvents.push({
        event_id: nextEventId++,
        task_id: id,
        event_type: completed ? "task_completed" : "task_reopened",
        event_time: now
    });

    res.json(task);
});

// =====================
// DELETE task
// =====================
app.delete("/tasks/:id", (req, res) => {
    const id = parseInt(req.params.id);
    const now = new Date().toISOString();

    tasks = tasks.filter(t => t.id !== id);

    taskEvents.push({
        event_id: nextEventId++,
        task_id: id,
        event_type: "task_deleted",
        event_time: now
    });

    res.json({ message: "Task deleted" });
});

// =====================
// GET task events
// =====================
app.get("/events", (req, res) => {
    res.json(taskEvents);
});

// =====================
// EXPORT + UPLOAD EVENTS
// =====================
app.get("/export/events", async (req, res) => {
    try {
        const date = new Date().toISOString().split("T")[0];
        const dir = path.join(__dirname, "exports");

        if (!fs.existsSync(dir)) {
            fs.mkdirSync(dir);
        }

        const fileName = `task_events_${date}.json`;
        const filePath = path.join(dir, fileName);

        fs.writeFileSync(filePath, JSON.stringify(taskEvents, null, 2));

        await uploadToAzure(
            filePath,
            `task_events/date=${date}/${fileName}`
        );

        res.json({
            message: "Events exported AND uploaded to Azure",
            record_count: taskEvents.length
        });
    } catch (err) {
        console.error(err);
        res.status(500).json({ error: err.message });
    }
});

// =====================
app.listen(PORT, () => {
    console.log(`Server running on http://localhost:${PORT}`);
});

