supplier=["FlexiGas","Nuke22","ShinyPower","RoskildeCHP","BlueWater","Zero"]
id=["G₁","G₂","G₃","G₄","G₅", "Z"]
quantity=[15,100,32,25,70,0]
price=[75,15,0,42,10,0]

bids = DataFrame(Supplier=supplier,ID=id,Quantity=quantity,Price=price)
sort!(bids,[:Price, :Quantity])
for p in bids.Quantity
    bids.Quantity[p]=bids.Quantity[p-1]+bids.Quantity[p]
end
bids
