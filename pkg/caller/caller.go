package caller

import (
	"runtime"
	"strings"
)

// Name возвращает название функции/метода который *вызвал вызывающую*
// функцию/метод
//
//	func Bar() {
//		callerName := caller.Name()
//		fmt.Println(callerName) // Bar
//	}
//
// -----------------------
//
//	func Foo() {
//		Bar()
//	}
//
//	func Bar() {
//		callerName := caller.Name(1)
//		fmt.Println(callerName) // Foo
//	}
func Name(offsetOpt ...int) string {
	offset := 1
	if len(offsetOpt) > 0 {
		offset += offsetOpt[0]
	}

	pc, _, _, ok := runtime.Caller(offset)
	details := runtime.FuncForPC(pc)

	if ok && details != nil {
		fullName := details.Name()
		parts := strings.Split(fullName, ".")

		if len(parts) == 0 {
			return "" // на всякий случай
		}

		// вызов из анонимной функии, нужно убрать последний элемент ("func1", "func2" и тд)
		if strings.HasPrefix(parts[len(parts)-1], "func") {
			parts = parts[:len(parts)-1]
		}

		if len(parts) == 0 {
			return "" // снова на всякий случай
		}

		if len(parts) > 2 { // это метод, например ["marketplaces/feed_service/pkg/caller", "(*testStruct)", "Method"]
			typeName := parts[len(parts)-2]
			methodName := parts[len(parts)-1]

			typeName = cleanTypeName(typeName)

			return strings.Join([]string{typeName, methodName}, ".") // вернем (*testStruct).Method
		}

		// это функция - просто возвращаем последний элемент пути
		funcName := parts[len(parts)-1]
		return funcName
	}

	return ""
}

func cleanTypeName(name string) string {
	return strings.Trim(name, "(*)") // "(*myStruct)" станет "myStruct"
}
