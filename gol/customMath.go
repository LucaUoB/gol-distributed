package gol

func Mod(d, m int) int {
	rem := d % m
	if (rem < 0 && m > 0) || (rem > 0 && m < 0) {
		return rem + m
	}
	return rem
}
