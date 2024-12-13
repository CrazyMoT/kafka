document.addEventListener("DOMContentLoaded", function () {
    const sendButton = document.getElementById("sendButton");

    sendButton.addEventListener("click", async function () {
        const data = {
            user_id: 1,
            timestamp: new Date().toISOString(),
            action_type: "button_click",
            page_url: window.location.href
        };

        try {
            const response = await fetch("/send_data", {
                method: "POST",
                headers: {
                    "Content-Type": "application/json"
                },
                body: JSON.stringify(data)
            });

            const result = await response.json();
        } catch (error) {
            console.log("Error sending message: " + error);
        }
    });
});
