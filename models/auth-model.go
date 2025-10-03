package models

type RegisterUserDto struct {
	Email string `json:"email" validate:"required,email"`
	Name string `json:"name" validate:"omitempty, min=2"`
	Password string `json:"password" validate:"required,min=6"`
}

type LoginUserDto struct {
	Email string `json:"email" validate:"required,email"`
	Password string `json:"password" validate:"required,min=8"`
}

type AuthResponse struct {
	Success bool `json:"success"`
	Token string `json:"token"`
	Email string `json:"email"`
}