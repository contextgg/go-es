package hydra

// Introspect if the token is valid
type Introspect struct {
	Active    bool   `json:"active"`
	Scope     string `json:"scope"`
	ClientID  string `json:"client_id"`
	Sub       string `json:"sub"`
	Iss       string `json:"iss"`
	TokenType string `json:"token_type"`
}

// IsAccessToken will return true if the token type is an access_token
func (i *Introspect) IsAccessToken() bool {
	return i.TokenType == "access_token"
}
