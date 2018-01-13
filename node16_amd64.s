// func node16FindChildAVX2(keys *byte, key byte, nc uint8) uint8
TEXT ·node16FindChildAVX2(SB),$0-17
    MOVQ          keys+0(FP), CX
    MOVOU         (CX), X0
    VPBROADCASTB  key+8(FP), X1
    PCMPEQB       X0, X1
    PMOVMSKB      X1, BX
    MOVBLZX       nc+9(FP), CX
    MOVW          $1, AX
    SHLW          CX, AX
    DECW          AX
    ANDW          AX, BX
    JZ            NF
    BSFW          BX, BX
    MOVB          BX, ret+16(FP)
    RET
NF:
    MOVB          CX, ret+16(FP)
    RET

// func supportAVX2() bool
TEXT ·supportAVX2(SB),$0-1
    MOVQ runtime·support_avx2(SB), AX
    MOVB AX, ret+0(FP)
    RET
