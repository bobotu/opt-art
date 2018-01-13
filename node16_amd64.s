// func node16FindChildASM(keys *byte, key byte) uint16
TEXT ·node16FindChildASM(SB),$0-18
    MOVQ          keys+0(FP), CX
    MOVOU         (CX), X0
    VPBROADCASTB  key+8(FP), X1
    PCMPEQB       X0, X1
    PMOVMSKB      X1, BX
    MOVW          BX, ret+16(FP)
    RET

// func supportSSE2() bool
TEXT ·supportSSE2(SB),$0-1
    MOVQ runtime·support_sse2(SB), AX
    MOVB AX, ret+0(FP)
    RET
