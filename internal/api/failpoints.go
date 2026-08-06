package api

import (
	"net/http"

	"github.com/adityarangi/payment-ledger/internal/apperr"
	"github.com/adityarangi/payment-ledger/internal/failpoint"
)

// These handlers are registered only when LEDGER_FAILPOINTS_ENABLED is set,
// so a production deployment does not expose them at all.

func (s *Server) handleListFailpoints(w http.ResponseWriter, r *http.Request) {
	s.writeJSON(w, r, http.StatusOK, map[string]any{
		"enabled":   s.failpoints.Enabled(),
		"available": failpoint.All,
		"active":    s.failpoints.Active(),
	})
}

func (s *Server) handleArmFailpoint(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Name   string `json:"name"`
		Action string `json:"action"`
	}
	if err := decodeJSON(r, &req); err != nil {
		s.writeError(w, r, err)
		return
	}
	if err := s.failpoints.Arm(req.Name, req.Action); err != nil {
		s.writeError(w, r, apperr.InvalidRequest("%s", err.Error()))
		return
	}
	s.writeJSON(w, r, http.StatusOK, map[string]any{"active": s.failpoints.Active()})
}

func (s *Server) handleResetFailpoints(w http.ResponseWriter, r *http.Request) {
	s.failpoints.Reset()
	s.writeJSON(w, r, http.StatusOK, map[string]any{"active": s.failpoints.Active()})
}
