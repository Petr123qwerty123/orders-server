package api

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"strconv"
	"sync"
	"wb-test-task/internal/db"

	"github.com/go-chi/chi/v5"
)

type ordkey string

const orderKey ordkey = "order"

type Api struct {
	rtr                *chi.Mux
	csh                *db.Cache
	name               string
	srv                *http.Server
	httpServerExitDone *sync.WaitGroup
}

func NewApi(csh *db.Cache) *Api {
	api := Api{}
	api.Init(csh)
	return &api
}

func (a *Api) Init(csh *db.Cache) {
	a.csh = csh
	a.name = "API"
	a.rtr = chi.NewRouter()

	a.rtr.Route("/orders", func(r chi.Router) {
		r.Route("/{orderID}", func(r chi.Router) {
			r.Use(a.orderCtx)
			r.Get("/", a.GetOrder)
		})
	})

	a.httpServerExitDone = &sync.WaitGroup{}
	a.httpServerExitDone.Add(1)
	a.StartServer()
}

func (a *Api) Finish() {
	log.Printf("%v: Выключение сервера...\n", a.name)

	if err := a.srv.Shutdown(context.Background()); err != nil {
		panic(err) // failure/timeout shutting down the server gracefully
	}

	a.httpServerExitDone.Wait()
	log.Printf("%v: Сервер успешно выключен!\n", a.name)
}

func (a *Api) StartServer() {
	a.srv = &http.Server{
		Addr:    ":8000",
		Handler: a.rtr,
	}

	go func() {
		defer a.httpServerExitDone.Done()

		log.Printf("%v: сервер будет запущен по адресу http://localhost:8000\n", a.name)

		if err := a.srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			log.Printf("ListenAndServe() error: %v", err)
			return
		}
	}()
}

func (a *Api) orderCtx(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		orderIDstr := chi.URLParam(r, "orderID")
		orderID, err := strconv.ParseInt(orderIDstr, 10, 64)
		if err != nil {
			log.Printf("%v: ошибка конвертации %s в число: %v\n", a.name, orderIDstr, err)
			http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
			return
		}

		log.Printf("%v: запрос OrderOut из кеша/бд, OrderID: %v\n", a.name, orderIDstr)
		orderOut, err := a.csh.GetOrderOutById(orderID)
		if err != nil {
			log.Printf("%v: ошибка получения OrderOut из базы данных: %v\n", a.name, err)
			http.Error(w, http.StatusText(http.StatusNotFound), http.StatusNotFound)
			return
		}
		ctx := context.WithValue(r.Context(), orderKey, orderOut)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func (a *Api) GetOrder(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	orderOut, ok := ctx.Value(orderKey).(*db.OrderOut)
	if !ok {
		log.Printf("%v: getOrder(): ошибка приведения к типу *OrderOut\n", a.name)
		http.Error(w, http.StatusText(http.StatusUnprocessableEntity), http.StatusUnprocessableEntity) // 422
		return
	}

	resp, err := json.Marshal(orderOut)
	if err != nil {
		log.Printf("%v: getOrder(): ошибка сериализации: %s\n", a.name, err)
		http.Error(w, "Internal Server Error", 500)
		return
	}

	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type")

	w.WriteHeader(http.StatusOK)
	w.Write(resp)
}
