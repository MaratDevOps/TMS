package generate

func DocumentCount(positionCount, positionsPerDocument int) int {
	if positionsPerDocument <= 0 {
		return 0
	}
	return (positionCount + positionsPerDocument - 1) / positionsPerDocument
}

func Assign(jobNumber, positionsPerDocument int) (documentIndex, positionNumber int) {
	documentIndex = (jobNumber - 1) / positionsPerDocument
	positionNumber = ((jobNumber - 1) % positionsPerDocument) + 1
	return
}
