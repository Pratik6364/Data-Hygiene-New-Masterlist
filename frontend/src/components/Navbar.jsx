import React, { useState } from "react";
import {
    AppBar,
    Toolbar,
    Typography,
    Box,
} from "@mui/material";
import { useNavigate } from "react-router-dom";
import { NavLink } from "react-router-dom";

const NAV_LINKS = [
    { label: "All", path: "/" },
    { label: "Active List", path: "/active" },
    { label: "Completed List", path: "/completed" },
    { label: "On Hold", path: "/on-hold" },
];

const Navbar = () => {
    const navigate = useNavigate();


    return (
        <AppBar position="fixed" sx={{ backgroundColor: "#17233a" }}>
            <Toolbar sx={{ justifyContent: "space-between" }}>
                {/* Logo */}
                <Typography
                    variant="h6"
                    fontWeight={700}
                    sx={{ cursor: "pointer", letterSpacing: 1 }}
                    onClick={() => navigate("/")}
                >
                    AMD_DH
                </Typography>

                {/* Dropdown */}
                <Box sx={{ display: "flex", gap: 4 }}>
                    {NAV_LINKS.map(({ label, path }) => (
                        <NavLink
                            key={label}
                            to={path}
                            style={({ isActive }) => ({
                                textDecoration: "none",
                                color: "#fff",
                                fontWeight: 600,
                                fontSize: "0.95rem",
                                opacity: isActive ? 1 : 0.7,
                                borderBottom: isActive ? "2px solid #fff" : "2px solid transparent",
                                paddingBottom: "4px",
                                transition: "all 0.2s ease",
                                fontFamily: "'Inter', sans-serif"
                            })}
                        >
                            {label}
                        </NavLink>
                    ))}
                </Box>
            </Toolbar>
        </AppBar>
    );
};

export default Navbar;