// func node16FindChildASM(keys *byte, key byte) uint16
TEXT ·node16FindChildASM(SB),$0-18
    MOVQ          keys+0(FP), CX
    MOVOU         (CX), X0
    VPBROADCASTB  key+8(FP), X1
    PCMPEQB       X0, X1
    PMOVMSKB      X1, BX
    MOVW          BX, ret+16(FP)
    RET
