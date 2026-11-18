package mongodb

import "fmt"

// intID is an int32 _id's text.
func intID(i int) string { return fmt.Sprintf(`{"$numberInt":"%d"}`, i) }
