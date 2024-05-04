package timerassistant

type Owner interface {
	Execute(func())
}
