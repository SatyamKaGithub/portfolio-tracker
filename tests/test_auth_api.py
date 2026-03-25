def test_signup_and_login_flow(client):
    signup_response = client.post(
        "/auth/signup",
        json={
            "username": "satyam",
            "email": "satyam@example.com",
            "password": "strongpass123",
        },
    )
    assert signup_response.status_code == 200

    login_response = client.post(
        "/auth/login",
        json={"login": "satyam@example.com", "password": "strongpass123"},
    )
    assert login_response.status_code == 200
    login_body = login_response.json()
    assert login_body["user"]["username"] == "satyam"
    assert "token" in login_body

    token = login_body["token"]

    me_response = client.get(
        "/auth/me",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert me_response.status_code == 200
    assert me_response.json()["email"] == "satyam@example.com"

    logout_response = client.post(
        "/auth/logout",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert logout_response.status_code == 200

    me_after_logout = client.get(
        "/auth/me",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert me_after_logout.status_code == 401


def test_signup_rejects_duplicate_user(client):
    payload = {
        "username": "satyam",
        "email": "satyam@example.com",
        "password": "strongpass123",
    }
    first = client.post("/auth/signup", json=payload)
    second = client.post("/auth/signup", json=payload)

    assert first.status_code == 200
    assert second.status_code == 400
    assert "already exists" in second.json()["detail"]


def test_login_supports_username_and_rejects_wrong_password(client):
    client.post(
        "/auth/signup",
        json={
            "username": "rahul_1",
            "email": "rahul@example.com",
            "password": "correctpass123",
        },
    )

    ok_response = client.post(
        "/auth/login",
        json={"login": "rahul_1", "password": "correctpass123"},
    )
    assert ok_response.status_code == 200
    assert ok_response.json()["user"]["email"] == "rahul@example.com"

    bad_response = client.post(
        "/auth/login",
        json={"login": "rahul_1", "password": "wrongpass123"},
    )
    assert bad_response.status_code == 401
    assert "Invalid credentials" in bad_response.json()["detail"]
