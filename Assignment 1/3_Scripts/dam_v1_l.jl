using DataFrames
using Plots
using JuMP
using Gurobi
using MathOptFormat
using LinearAlgebra
using Printf
using CSV
using Dates
gr()

#Read data
files=["consumption-prognosis_2019_hourly.csv","consumption-prognosis_2020_hourly.csv","wind-power-dk-prognosis_2019_hourly.csv","wind-power-dk-prognosis_2020_hourly.csv"]

function readandclean(file)
    df = CSV.read(file,header=3,datarow=4)
    df = rename!(df,:1=>"Date")
    return df
end

df_consumption2019 = readandclean(files[1])
df_consumption2020 = readandclean(files[2])
df_wind2019 = readandclean(files[3])
df_wind2020 = readandclean(files[4])
#check for missing values
#=Data to arrays?
array_b = Array(df_b)
b = zeros(Int,length(M),length(F),length(T),length(S))
for i=1:size(array_b,1)
    b[array_b[i,1],array_b[i,2],array_b[i,3],array_b[i,4]]=array_b[i,5]
end
=#

supplier_DK1 = ["FlexiGas","FlexiGas","FlexiGas","Peako","Peako","Nuke22","CoalAtLast"]
id_GDK1 = ["G₁","G₂","G₃","G₄","G₅","G₆","G₇"]
supplier_DK2 = ["Nuke22","RoskildeCHP","RoskildeCHP","Avedøvre","Avedøvre","BlueWater","BlueWater","CoalAtLast"]
id_GDK2 = ["G₈","G₉","G₁₀","G₁₁","G₁₂","G₁₃","G₁₄","G₁₅"]
#creating imports and exports
en_NO = repeat([-100],24)
en_GE=zeros(24)
for i=1:24
    if (i<9 || i>15)
        en_GE[i] = 0
    else
        en_GE[i] = 120
end
end
en_SWE=zeros(24)
for i=1:24
    if (i<11 || i>17)
        en_SWE[i] = 0
    else
        en_SWE[i] = -80
end
end
# Initialize vectors
#N_D = size(consumer,1)
#n_D = collect(1:N_D)
#n = collect(1:(N_G+N_D))
PG_DK1  = [380,350,320,370,480,900,1200]
λG_DK1 = [72,62,150,80,87,24,260]
PG_DK2  = [1100,300,380,360,320,750,600,860]
λG_DK2 = [17,44,40,37,32,5,12,235]
#c = vcat(λ_G,-λ_D)
#A_eq = transpose(vcat(ones(N_G),-ones(N_D)))
#A = Array(Diagonal(ones(N_G+N_D)))
#b = vcat(P_G,P_D)
trans_limit = 600

# Model
ass1 = Model(with_optimizer(Gurobi.Optimizer))
@variable(ass1, y[j in n] >= 0)
@objective(ass1, Min, transpose(c)*y)
@constraint(ass1, generationDK1, A*y .<= b)
@constraint(ass1, generationDK2, A*y .<= b)
@constraint(ass1, balanceDK1[t], A_eq*y + en_GE[t] + en_NO[t] == trans_limit)
@constraint(ass1, balanceDK2[t], A_eq*y + en_SWE[t] == -trans_limit)
optimize!(ass1)

# Model output
if termination_status(ass1) == MOI.OPTIMAL
    println("Optimal solution found!\n")
    println("Generation and Demand:\n")
    for j in n_G
        println("$(id_G[j]): ", value.(y[j]), " MWh")
    end
    for i in n_D
        println("$(id_D[i]): ", value.(y[i]), " MWh")
    end
    println("\nObjective value: ", objective_value(ass1), " €")
    println("\nMarket equilibrium: ", dual(balance), " €/MWh")
    else
        error("No solution.")
end

power = Array(n)
for k in n
    power[k]=value.(y)[k]
end

# Data Frames
df_G = DataFrame(Supplier=supplier,ID_G=id_G,Offer_G=P_G,Price_G=λ_G,Schedule_G=power[1:N_G],Market_G=fill(dual(balance),N_G))
df_D = DataFrame(Consumer=consumer,ID_D=id_D,Offer_D=P_D,Price_D=λ_D,Schedule_D=power[N_G+1:maximum(n)],Market_D=fill(dual(balance),N_D))
df_G.PayAsBid_G = df_G.Schedule_G.*df_G.Price_G
df_G.UniformPricing_G = df_G.Schedule_G.*df_G.Market_G
df_D.PayAsBid_D = df_D.Schedule_D.*df_D.Price_D
df_D.UniformPricing_D = df_D.Schedule_D.*df_D.Market_D

#plot(bidsSupplier.AggregatedQ,bidsSupplier.Price,w=2,t=:steppre, xlim=(0,sum(bidsSupplier.Quantity)), xlab="Quantity [MWh]", ylab="Price [EUR/MWh]", color="darkred", legend=false)
