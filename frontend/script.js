const API_URL = "http://localhost:3000/tasks";

async function loadTasks() {
    const res = await fetch(API_URL);
    const tasks = await res.json();

    const table = document.querySelector("#taskTable tbody");
    table.innerHTML = "";

    tasks.forEach(task => {
        table.innerHTML += `
            <tr>
                <td>${task.id}</td>
                <td>${task.title}</td>
                <td>${task.description}</td>
                <td>${task.completed ? "✔️" : "❌"}</td>
                <td>
                    <button onclick="toggleComplete(${task.id}, ${!task.completed})">
                        ${task.completed ? "Mark Incomplete" : "Mark Complete"}
                    </button>
                    <button onclick="deleteTask(${task.id})" style="color:red">Delete</button>
                </td>
            </tr>
        `;
    });
}

async function createTask() {
    const title = document.getElementById("title").value;
    const description = document.getElementById("description").value;

    await fetch(API_URL, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ title, description })
    });

    loadTasks();
}

async function toggleComplete(id, completed) {
    await fetch(`${API_URL}/${id}`, {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ completed })
    });

    loadTasks();
}

async function deleteTask(id) {
    await fetch(`${API_URL}/${id}`, {
        method: "DELETE"
    });

    loadTasks();
}

loadTasks();
