package judge0

import "strings"

func LanguageID(language string) (int, error) {
	switch strings.ToLower(strings.TrimSpace(language)) {
	case "python", "python3", "py":
		return 71, nil
	case "cpp", "c++":
		return 54, nil
	case "c":
		return 50, nil
	case "java":
		return 62, nil
	case "javascript", "js", "node":
		return 63, nil
	case "go", "golang":
		return 60, nil
	default:
		return 0, ErrUnsupportedLanguage
	}
}
func CompilerOptions(languageID int) string {
	if languageID == 54 {
		return "-std=c++17"
	}
	return ""
}
